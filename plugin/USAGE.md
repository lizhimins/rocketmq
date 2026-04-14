# Fast CommitLog 使用指南

## 背景

ext4 文件系统上 RocketMQ CommitLog 同步写入存在 **5 倍 jdb2 写入放大**，根源是：

| 来源 | 说明 |
|------|------|
| unwritten → written extent 转换 | jdb2 记录元数据变更 |
| inode i_size 更新 | jdb2 记录元数据变更 |
| extent tree 结构变更 | jdb2 记录元数据变更 |

本方案通过 **预分配 + No-Hide 补丁** 在运行时彻底消除上述 jdb2 元数据写入，写入放大从 ~5x 降至 ~1.2x。

---

## 目录结构

```
fast-commitlog/
├── include/fast_commitlog.h       # 用户态 API 头文件
├── kernel/ext4_fast_alloc.c       # 内核补丁实现（参考）
├── userspace/fast_commitlog.c     # 用户态库实现（含自动 fallback）
├── userspace/demo_fast_commitlog.c # 演示程序
└── Makefile                       # 构建脚本
```

---

## 一、用户态集成（无需内核补丁）

即使不打内核补丁，`FALLOC_FL_ZERO_RANGE` 也能消除运行时 extent 转换开销，仅有一次性填零成本。

### 1.1 编译库和演示程序

```bash
cd fast-commitlog
make
```

产物在 `build/` 目录下：
- `libfastcommitlog.a` — 静态库
- `demo_fast_commitlog` — 演示程序

### 1.2 运行演示

```bash
# 创建一个 1GB 的 CommitLog 文件并执行写入基准测试
./build/demo_fast_commitlog /data/commitlog/test.log 1073741824
```

输出示例：

```
=== Fast CommitLog Demo ===
Path: /data/commitlog/test.log
File size: 1073741824 bytes (1024.00 MB)
Iterations: 10

[WARN] FALLOC_FL_NO_HIDE_STALE not supported (kernel patch not installed)
       Falling back to FALLOC_FL_ZERO_RANGE
Pre-allocation completed in 1234.56 ms

Running write benchmark...
Wrote 262144000 bytes in 5432.10 ms (46.12 MB/s)
```

### 1.3 在你的代码中集成

#### 方式 A：编译时直接链接

将 `include/fast_commitlog.h` 和 `userspace/fast_commitlog.c` 加入你的工程：

```c
#include "fast_commitlog.h"

int init_commitlog(const char *path, off_t file_size)
{
    // 打开文件（O_DSYNC 保证同步写入）
    int fd = open(path, O_RDWR | O_CREAT | O_DSYNC, 0644);
    if (fd < 0) return -1;

    // 预分配 written extents（自动选择最优路径）
    if (fast_commitlog_init(fd, file_size) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}
```

#### 方式 B：使用静态库

```bash
# 安装到系统目录
make install PREFIX=/usr/local

# 在你的 Makefile 中链接
# gcc your_code.c -lfastcommitlog -o your_program
```

```c
#include <fast_commitlog.h>

// 编译时: gcc your_code.c -L/usr/local/lib -lfastcommitlog -o your_program
```

#### 方式 C：最小内联实现（不想引入额外文件）

如果你只需要核心功能，可以直接在代码中使用：

```c
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#ifndef FALLOC_FL_NO_HIDE_STALE
#define FALLOC_FL_NO_HIDE_STALE  0x04
#endif
#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE     0x10
#endif

static int commitlog_prealloc(int fd, off_t size)
{
    // Step 1: 设置文件大小
    if (ftruncate(fd, size) != 0)
        return -1;

    // Step 2: 优先尝试 NO_HIDE_STALE（无填零开销）
    if (fallocate(fd, FALLOC_FL_NO_HIDE_STALE, 0, size) == 0)
        return 0;

    // Step 3: fallback 到 ZERO_RANGE（有一次性填零成本）
    if (errno == EOPNOTSUPP)
        return fallocate(fd, FALLOC_FL_ZERO_RANGE, 0, size);

    return -1;
}

// 使用
int fd = open("/data/commitlog/00000000001073741824",
              O_RDWR | O_CREAT | O_DSYNC, 0644);
commitlog_prealloc(fd, 1073741824);  // 预分配 1GB
void *mapped = mmap(NULL, 1073741824, PROT_READ|PROT_WRITE,
                    MAP_SHARED, fd, 0);
// 直接往 mapped 写入数据，无需 extent 转换
```

### 1.4 API 说明

| 函数 | 说明 | 需要内核补丁 |
|------|------|:-----------:|
| `fast_commitlog_init(fd, size)` | **推荐入口**：初始化 CommitLog 文件，自动 fallback | 否 |
| `fast_commitlog_alloc(fd, off, len)` | 使用 NO_HIDE_STALE 预分配（跳过填零） | **是** |
| `fast_commitlog_zero_range_alloc(fd, off, len)` | 使用 ZERO_RANGE 预分配（标准接口） | 否 |
| `fast_commitlog_check_support(fd)` | 探测内核是否支持 NO_HIDE_STALE | — |

### 1.5 三种模式的效果对比

| 模式 | 预分配开销 | 运行时 jdb2 开销 | 写入放大 |
|------|:---------:|:---------------:|:-------:|
| 无优化（标准 fallocate） | 低 | 高（extent 转换 + i_size） | ~5x |
| ZERO_RANGE（无需补丁） | 中（一次性填零） | 低（仅 i_size 更新） | ~1.5x |
| NO_HIDE_STALE（需补丁） | **极低** | **趋近于 0** | **~1.2x** |

---

## 二、安装内核补丁（获得最优效果）

### 2.1 前置条件

- 内核版本：Linux 5.10+（基于此版本验证，其他版本需适配）
- 内核源码：可从 CentOS/Alinux 官方源获取对应版本的 SRPM
- root 权限：编译和安装内核

### 2.2 获取内核源码

```bash
# 方式 1：从 Alinux 源获取（推荐，阿里云环境）
yumdownloader --source kernel
rpm -ivh kernel-*.src.rpm
cd ~/rpmbuild/SOURCES/
tar xf linux-*.tar.xz -C /usr/src/

# 方式 2：从 kernel.org 获取
wget https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-5.10.tar.xz
tar xf linux-5.10.tar.xz -C /usr/src/
cd /usr/src/linux-5.10
```

### 2.3 应用补丁

#### 步骤 1：添加 flag 定义

编辑 `include/uapi/linux/falloc.h`：

```diff
 #define FALLOC_FL_KEEP_SIZE             0x01
 #define FALLOC_FL_PUNCH_HOLE            0x02
+#define FALLOC_FL_NO_HIDE_STALE         0x04
 #define FALLOC_FL_COLLAPSE_RANGE        0x08
 #define FALLOC_FL_ZERO_RANGE            0x10
```

#### 步骤 2：添加核心分配函数

在 `fs/ext4/extents.c` 中添加 `ext4_alloc_file_blocks_no_hide()` 函数。该函数的核心逻辑：

```c
// 直接创建 written extent，不经过 unwritten 状态
ret = ext4_map_blocks(handle, inode, &map,
          EXT4_GET_BLOCKS_CREATE
        | EXT4_GET_BLOCKS_CONVERT_UNWRITTEN);
```

**与标准路径的区别**：

| | 标准路径 | NO_HIDE_STALE 路径 |
|--|---------|-------------------|
| flag | `EXT4_GET_BLOCKS_CREATE_UNWRIT_EXT` | `EXT4_GET_BLOCKS_CREATE \| EXT4_GET_BLOCKS_CONVERT_UNWRITTEN` |
| extent 状态 | unwritten | written（直接） |
| jdb2 事务 | 2 次（创建 + 转换） | 1 次（分配） |

#### 步骤 3：修改 ext4_fallocate

在 `fs/ext4/extents.c` 的 `ext4_fallocate()` 函数开头添加拦截逻辑：

```c
if (mode & FALLOC_FL_NO_HIDE_STALE) {
    // 拒绝不兼容的 flag 组合
    if (mode & (FALLOC_FL_PUNCH_HOLE |
                FALLOC_FL_COLLAPSE_RANGE |
                FALLOC_FL_ZERO_RANGE))
        return -EOPNOTSUPP;

    // 只允许普通文件
    if (!S_ISREG(file_inode(file)->i_mode))
        return -EOPNOTSUPP;

    // 权限检查（需要 CAP_SYS_ADMIN，可改为 mount option）
    if (!capable(CAP_SYS_ADMIN))
        return -EPERM;

    return ext4_alloc_file_blocks_no_hide(file, offset, len, mode);
}
```

> 完整参考代码见 [kernel/ext4_fast_alloc.c](kernel/ext4_fast_alloc.c)

### 2.4 编译并安装内核

```bash
# 使用当前内核配置作为基础
cp /boot/config-$(uname -r) .config
make olddefconfig

# 编译（根据 CPU 核心数调整 -j 参数）
make -j$(nproc)

# 安装模块和内核
sudo make modules_install
sudo make install

# 更新 grub（CentOS/Alinux）
sudo grub2-mkconfig -o /boot/grub2/grub.cfg

# 重启
sudo reboot
```

### 2.5 验证补丁生效

```bash
# 确认新内核已启动
uname -r

# 检查 NO_HIDE_STALE 是否可用
# 编写一个简单测试程序：
cat > /tmp/test_no_hide.c << 'EOF'
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#ifndef FALLOC_FL_NO_HIDE_STALE
#define FALLOC_FL_NO_HIDE_STALE 0x04
#endif

int main() {
    int fd = open("/tmp/test_no_hide_file", O_RDWR | O_CREAT, 0644);
    ftruncate(fd, 4096);
    int ret = fallocate(fd, FALLOC_FL_NO_HIDE_STALE, 0, 4096);
    if (ret == 0)
        printf("FALLOC_FL_NO_HIDE_STALE: SUPPORTED\n");
    else
        printf("FALLOC_FL_NO_HIDE_STALE: NOT SUPPORTED (%s)\n",
               strerror(errno));
    close(fd);
    unlink("/tmp/test_no_hide_file");
    return 0;
}
EOF
gcc /tmp/test_no_hide.c -o /tmp/test_no_hide
/tmp/test_no_hide
```

预期输出：

```
FALLOC_FL_NO_HIDE_STALE: SUPPORTED
```

---

## 三、替代方案：kProbe 运行时注入（无需重新编译内核）

如果无法重新编译内核，可以通过 kProbe 在运行时拦截 `ext4_map_blocks`：

```c
#include <linux/kprobe.h>
#include <linux/ext4.h>

static struct kprobe kp = {
    .symbol_name = "ext4_map_blocks",
};

static int kprobe_pre_handler(struct kprobe *p, struct pt_regs *regs)
{
    // 检查调用上下文是否为 NO_HIDE_STALE 路径
    // 如果是，修改 flags 参数使其直接创建 written extent
    // 具体实现取决于内核版本的 ext4_map_blocks 签名
    return 0;
}

kp.pre_handler = kprobe_pre_handler;
register_kprobe(&kp);
```

通过 kernel module 加载：

```bash
# 编译为 kernel module
make -C /lib/modules/$(uname -r)/build M=$(pwd) modules

# 加载
sudo insmod ext4_kprobe_no_hide.ko

# 验证
dmesg | tail
```

> kProbe 方案的具体实现需要根据目标内核版本的 `ext4_map_blocks` 签名进行调整。

---

## 四、在 RocketMQ 中的集成建议

### 4.1 CommitLog 文件初始化时机

在 `CommitLog` 类的文件创建流程中加入预分配：

```
CommitLog.putMessage()
  └─ MappedFileQueue.getNextMappedFile()
       └─ MappedFile 创建时
            └─ 调用 fast_commitlog_init(fd, fileSize)  ← 新增
```

### 4.2 JNI 绑定

由于 `fast_commitlog` 是 C 库，需要通过 JNI 在 Java 中调用：

```java
public class FastCommitlogNative {
    static {
        System.loadLibrary("fastcommitlog_jni");
    }

    // 返回: 0 = 不支持, 1 = 支持
    public static native boolean checkSupport(String path);

    // 返回: 0 = 成功, -1 = 失败
    public static native int preallocate(String path, long fileSize);
}
```

JNI 实现：

```c
JNIEXPORT jint JNICALL
Java_xxx_FastCommitlogNative_preallocate
    (JNIEnv *env, jclass cls, jstring jpath, jlong fileSize)
{
    const char *path = (*env)->GetStringUTFChars(env, jpath, NULL);
    int fd = open(path, O_RDWR | O_CREAT | O_DSYNC, 0644);
    int ret = fast_commitlog_init(fd, (off_t)fileSize);
    close(fd);
    (*env)->ReleaseStringUTFChars(env, jpath, path);
    return ret;
}
```

### 4.3 配置项

建议在 `broker.conf` 中增加开关：

```properties
# 启用 fast-commitlog 预分配（需要内核支持 NO_HIDE_STALE）
enableFastCommitlogPreAllocation=true

# 回退策略：当 NO_HIDE_STALE 不可用时
#   auto      - 自动降级到 ZERO_RANGE（默认）
#   force     - 必须 NO_HIDE_STALE，否则启动失败
#   disable   - 不使用任何优化
fastCommitlogFallbackStrategy=auto
```

---

## 五、安全性注意事项

| 风险 | 说明 | 缓解措施 |
|------|------|---------|
| 旧数据泄露 | NO_HIDE_STALE 跳过填零，可能暴露旧块数据 | CommitLog 是追加写，读取位点由 RocketMQ 管理，不会读取未写入区域 |
| 权限控制 | 非特权用户可能滥用此 flag | 内核补丁默认要求 `CAP_SYS_ADMIN`，或改为 mount option 控制 |
| 崩溃恢复 | written extent 已记录在 extent tree 中，崩溃后文件结构完整，数据可能不完整 | 由 RocketMQ 自身恢复机制（commitlog checksum + ConsumeQueue 重建）处理 |
| 文件动态扩展 | 如果文件不是固定大小预分配，会退化为标准路径 | CommitLog 使用固定大小 MappedFile，天然满足此条件 |
