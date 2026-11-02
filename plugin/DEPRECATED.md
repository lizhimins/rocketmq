# DEPRECATED — 本目录已被取代，不要构建、不要加载

**状态：2026-10-16 起废弃。** 取代它的是 `terrances/project/rocketmq/features/commitlog/kmod/`
（内核模块 `ext4_prewrite.ko` + C 库 `libext4prewrite` + JNI glue）与 RocketMQ 仓库的
`store/src/main/java/org/apache/rocketmq/store/prewrite/Ext4Prewrite.java`。

## 为什么废弃

本目录有两条路线，都不如取代者：

1. `kernel/ext4_fast_alloc.c` 是**内核补丁**，给 ext4 加 `FALLOC_FL_NO_HIDE_STALE` 的原生实现。
   需要重编内核才能用，而目标环境（ACK 节点 `5.10.134-19.8.al8`）不可能重编内核，路线不成立。
2. `kmodule/ext4_fast_alloc.c` 用 **kprobe 挂 `ext4_fallocate` 与 `ext4_map_blocks` 两个热函数**，
   在飞行中按线程上下文改写 flags。问题：
   - 钩子对**全系统所有 ext4 I/O** 生效，不止 CommitLog；每次 fallocate / map_blocks 都要走
     kprobe 陷阱，稳态代价无法忽略，且无法只对本 Broker 生效。
   - 靠线程局部上下文在 pre/post handler 之间传状态，任何绕过被钩函数的调用路径
     （direct IO、writeback、fsync 触发的 map_blocks）都会拿到错误或缺失的上下文。
   - 从未做过写放大、恢复安全或崩溃注入验证。

取代者的差别是**热路径零钩子**：不挂任何内核函数，只在创建 CommitLog 文件时通过字符设备
ioctl 调一次真实内核函数 `ext4_convert_unwritten_extents(handle=NULL, …)`，由内核自己分段管理
journal credits（见报告 4.2 第 2 点）。该实现已在 `-19.2` 与 `-19.8` 两个内核上做过受控验证：
写放大 6.00× → 1.00×、jbd2 事务归零、转换过程落盘 0 字节，并有恢复安全的注入测试。

## 约束

- **不要 `insmod` 本目录的 kmodule**，尤其不要与 `ext4_prewrite` 同时加载：两者都会改变
  ext4 的分配/转换行为，叠加后无法归因任何测量结果。
- 本目录保留只为记录探索过程；实验结束后整体删除（git 历史可查）。
- 新的讨论与改动一律去 `kmod/` 与 `store/.../prewrite/`，不要在本文档继续演进。
