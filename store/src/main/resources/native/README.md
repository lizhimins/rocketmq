# ConsumeQueue RocksDB Native Compaction Filter Shim

## 编译方法

### 前置依赖

| 依赖 | 来源 |
|------|------|
| JDK 11+ headers | `/usr/lib/jvm/java-11-openjdk-...` |
| RocksDB 头文件 | 与 rocksdbjni 版本一致，如 `/tmp/rocksdb-8.4.4/include` |
| `librocksdbjni-linux64.so` | 从 rocksdbjni JAR 中提取：`unzip -p rocksdbjni-8.4.4.jar librocksdbjni-linux64.so` |

### 编译命令

```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-<version>
ROCKSDB_INCLUDE=/tmp/rocksdb-8.4.4/include
ROCKSDBJNI_LIB=/tmp/rocksdb-link   # 存放 librocksdbjni-linux64.so 的目录

mkdir -p "$ROCKSDBJNI_LIB"
unzip -o ~/.m2/repository/org/rocksdb/rocksdbjni/8.4.4/rocksdbjni-8.4.4.jar \
    librocksdbjni-linux64.so -d "$ROCKSDBJNI_LIB"

g++ -shared -fPIC -O2 -std=c++17 -fno-rtti -D_GLIBCXX_USE_CXX11_ABI=0 \
  -I"$JAVA_HOME/include" \
  -I"$JAVA_HOME/include/linux" \
  -I"$ROCKSDB_INCLUDE" \
  cq_compaction_filter.cpp \
  -L"$ROCKSDBJNI_LIB" -lrocksdbjni-linux64 \
  -Wl,-rpath,'$ORIGIN' \
  -o libcq_compaction_filter.so
```

### 关键编译参数说明

| 参数 | 原因 |
|------|------|
| `-std=c++17` | RocksDB 8.4.4 头文件使用 `std::string_view`、`std::make_from_tuple`，需 C++17 |
| `-fno-rtti` | rocksdbjni 的 .so 不导出 typeinfo 符号（`_ZTIN7rocksdb12CustomizableE`），开启 RTTI 会导致运行时 undefined symbol |
| `-D_GLIBCXX_USE_CXX11_ABI=0` | rocksdbjni 使用旧 C++ ABI（`_ZNSs` 而非 `_ZNSt7__cxx11...`），ABI 不匹配时运行时符号解析失败 |
| `-lrocksdbjni-linux64` + `-Wl,-rpath,'$ORIGIN'` | 将 rocksdbjni 的 .so 设为 DT_NEEDED 依赖，并通过 `$ORIGIN` rpath 使动态链接器在 .so 所在目录查找 |

### 验证

编译完成后检查 DT_NEEDED 和符号表：

```bash
# 确认 DT_NEEDED 中包含相对名称的 rocksdbjni 库
readelf -d libcq_compaction_filter.so | grep NEEDED

# 应输出：
#   [NEEDED] Shared library: [librocksdbjni-linux64.so]
#   [NEEDED] Shared library: [libstdc++.so.6]
#   ...
#   [RPATH]  Library rpath: [$ORIGIN]

# 确认未引用 typeinfo 符号
nm -D libcq_compaction_filter.so | grep "_ZTI"
# 应无输出
```
