# 问题描述

在Linux ext4文件系统环境下发现RocketMQ CommitLog同步写入存在5倍jdb2写入放大问题，
依次探讨了写放大的来源（unwritten extent转换、inode更新、extent tree变更）、
规避方案（换XFS、fallocate预热、FALLOC_FL_NO_HIDE_STALE等）、
非标准内核特性的识别与测试方法（FALLOC_FL_WRITE_ZEROES的验证手段）、
以及在不换文件系统不改挂载参数的约束下通过kProbe、ftrace函数替换、
预分配+No-Hide补丁等内核级手段彻底消除运行时jdb2元数据写入开销的可行性与实现路径。

# 预分配 + No-Hide 补丁方案分析

## 核心思路验证

```
当前写放大来源：
① unwritten → written extent 转换  → jdb2 记录元数据
② inode i_size 更新               → jdb2 记录元数据  
③ extent tree 结构变更             → jdb2 记录元数据

你的方案：
① fallocate 预分配 → 消除运行时 extent 转换
② No-Hide 补丁    → 跳过填零流程，直接 written
                  → 从根本上消除 ① 和 ③ 的 jdb2 开销
```

**结论：思路是正确且可行的。**

---

## 两个子问题拆解

### 子问题①：预分配消除 unwritten extent 转换

```c
// 标准 fallocate 预分配
fallocate(fd, 0, 0, FILE_SIZE);

/*
 * 此时 extent 状态：
 *
 *  [0        ~       1GB]
 *  └── unwritten extent ──┘
 *           ↑
 *  写入时仍然需要转换，jdb2 仍然记录
 *
 * 问题没有彻底解决！
 */
```

```c
// 使用 FALLOC_FL_ZERO_RANGE
fallocate(fd, FALLOC_FL_ZERO_RANGE, 0, FILE_SIZE);

/*
 * 此时 extent 状态：
 *
 *  [0        ~       1GB]
 *  └──  written extent  ──┘
 *           ↑
 *  写入时不触发转换，但填零本身产生一次性开销
 *  之后业务写入 jdb2 不再记录 extent 状态变更
 */
```

---

### 子问题②：No-Hide 机制的本质

```
标准内核填零流程：

fallocate(fd, 0, offset, len)
        │
        ▼
分配 unwritten extent
        │
        ▼
实际写入时触发：
  ext4_ext_handle_unwritten_extents()
        │
        ▼
  内核必须填零（防止旧数据泄露）
        │
        ├── 软件填零：内核 memset → 额外写入开销
        └── 硬件填零：WRITE SAME 指令（需硬件支持）
        │
        ▼
  jdb2 记录 extent 状态转换
        │
        ▼
  写放大
```

```
No-Hide 机制：

fallocate(fd, FALLOC_FL_NO_HIDE_STALE, offset, len)
        │
        ▼
直接分配 written extent
        │
        ▼
跳过填零流程
        │
        ▼
不记录 extent 状态转换
        │
        ▼
jdb2 无需提交元数据 ← 写放大消除
```

---

## 自己实现补丁的完整方案

### 补丁核心：修改 ext4_fallocate 路径

```c
// fs/ext4/extents.c

// 新增 flag 定义
// include/uapi/linux/falloc.h
#define FALLOC_FL_NO_HIDE_STALE  0x04  // 与阿里云内核保持一致

// 修改 ext4_fallocate
static long ext4_fallocate(struct file *file,
                            int mode,
                            loff_t offset,
                            loff_t len)
{
    // 检测到 NO_HIDE_STALE flag
    if (mode & FALLOC_FL_NO_HIDE_STALE) {
        /*
         * 直接分配 written extent
         * 跳过填零流程
         * 调用方保证不会读取未写入区域
         */
        return ext4_alloc_file_blocks_no_hide(
            file, offset, len, mode);
    }

    // 原有逻辑不变
    return ext4_alloc_file_blocks(file, offset, len, ...);
}
```

### 关键函数：直接分配 written extent

```c
static int ext4_alloc_file_blocks_no_hide(
    struct file *file,
    loff_t offset,
    loff_t len,
    int mode)
{
    struct inode *inode = file_inode(file);
    struct ext4_map_blocks map;
    handle_t *handle;
    int ret;

    // 开启 jdb2 事务（只需要一次，分配元数据）
    handle = ext4_journal_start(inode,
                EXT4_HT_MAP_BLOCKS,
                ext4_chunk_trans_blocks(inode, len >> 12));

    map.m_lblk  = offset >> inode->i_blkbits;
    map.m_len   = len >> inode->i_blkbits;

    // 关键：使用 EXT4_GET_BLOCKS_CREATE 而非默认标志
    // 直接创建 written extent，不经过 unwritten 状态
    ret = ext4_map_blocks(handle, inode, &map,
                          EXT4_GET_BLOCKS_CREATE
                        | EXT4_GET_BLOCKS_CONVERT_UNWRITTEN);
    /*
     * 与标准路径的区别：
     *
     * 标准路径：
     *   EXT4_GET_BLOCKS_CREATE_UNWRIT_EXT
     *   → 创建 unwritten extent
     *   → 写入时再转换
     *   → 两次 jdb2 事务
     *
     * 补丁路径：
     *   EXT4_GET_BLOCKS_CREATE
     *   → 直接创建 written extent
     *   → 写入时无需转换
     *   → 只有一次 jdb2 事务（分配时）
     */

    ext4_journal_stop(handle);
    return ret;
}
```

---

## 与你方案的对应关系

```
你的方案：
┌─────────────────────────────────────────┐
│                                         │
│  步骤1：预先指定好文件大小               │
│    fallocate(fd, NO_HIDE, 0, FILE_SIZE) │
│    → 一次性 jdb2 事务                   │
│    → 之后无 extent 转换开销             │
│                                         │
│  步骤2：业务写入                         │
│    write(fd, data, len)                 │
│    → extent 已是 written 状态           │
│    → 只有 i_size 更新的 jdb2 开销       │
│                                         │
│  步骤3：消除 i_size 更新开销            │
│    文件大小固定，i_size 不变            │
│    → jdb2 完全无需记录                  │
│                                         │
└─────────────────────────────────────────┘

最终 jdb2 开销：
  初始分配时：一次性开销（可以接受）
  业务写入时：趋近于 0
```

---

## 安全性保证

```
使用该补丁的前提条件：

① 调用方必须保证先写后读
   CommitLog 的顺序写入模型天然满足此条件

② 文件必须完整预分配
   不能动态扩展，否则退化为标准路径

③ 崩溃恢复
   written extent 已记录在 extent tree 中
   崩溃后文件结构完整
   只是数据内容可能不完整（由 RocketMQ 自身恢复机制处理）

④ 数据安全
   旧数据泄露风险：
   CommitLog 是追加写，读取位点由 RocketMQ 自己管理
   不会读取未写入区域，旧数据不会泄露给业务
```

---

## 最终效果预估

```
当前（data=ordered，无优化）：
  业务写入：  10M/s
  jdb2 写入： 50M/s
  总写入：    100M/s（含其他放大）
  放大倍数：  5x

应用补丁后：
  业务写入：  10M/s
  jdb2 写入： ~0（预分配时一次性完成）
  总写入：    ~12M/s（少量不可避免的元数据）
  放大倍数：  ~1.2x
```

---

## 总结

| 机制 | 作用 | 是否需要补丁 |
|------|------|------------|
| fallocate 预分配 | 固定文件大小，消除 i_size 更新 | ❌ 标准接口 |
| FALLOC_FL_ZERO_RANGE | 创建 written extent，一次性填零 | ❌ 标准接口 |
| FALLOC_FL_NO_HIDE_STALE | 跳过填零，直接 written | ✅ 需要补丁 |
| 三者组合 | 彻底消除运行时 jdb2 开销 | ✅ 最优方案 |

> **你的思路完全正确**：预分配固定大小 + No-Hide 机制跳过填零，
> 是在不更换文件系统、不修改挂载参数前提下，
> 从内核层面彻底解决 ext4 jdb2 写放大的最优路径。