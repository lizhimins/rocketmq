# Replace Forked RocksDB JNI with Official Artifact

## Background

RocketMQ previously depended on a custom-forked RocksDB Java binding published as `org.apache.rocketmq:rocketmq-rocksdb:1.0.6`. This fork was maintained in the `apache/rocketmq-externals` repository and was essentially a republished copy of `org.rocksdb:rocksdbjni` with exactly **one** additional class:

- `org.rocksdb.RemoveConsumeQueueCompactionFilter` — a RocksDB compaction filter that removes stale consume queue entries during compaction. Its C++ implementation and JNI glue lived in the forked C++ source tree under `utilities/compaction_filters/remove_consumequeue_compactionfilter.*` and `java/rocksjni/remove_consumequeue_compactionfilterjni.cc`.

All other RocketMQ subsystems using RocksDB (Pop consumption state, config storage, index storage, timer storage, transaction half-message storage) used only standard RocksDB Java APIs and had no dependency on the fork's custom code.

## Problem

Maintaining a fork of RocksDB's Java bindings has several costs:

1. **Upgrade friction** — every RocksDB upstream release requires rebuilding the entire fork to pick up the new native library and Java API
2. **Native build complexity** — the fork bundles a full C++ build pipeline for multiple platforms (Linux glibc/musl, macOS, Windows)
3. **Dependency duplication** — the `rocksdb/` module in the RocketMQ source tree duplicates ~190 classes that are identical to upstream `rocksdbjni`
4. **License ambiguity** — the fork republishes Facebook's RocksDB code under the Apache group

## Solution

Replace `rocketmq-rocksdb` with the official `org.rocksdb:rocksdbjni:8.4.4` and move the single custom compaction filter into a standalone native shim.

### Why a native shim is needed

The official `rocksdbjni` provides a `ColumnFamilyOptions.setCompactionFilter(AbstractCompactionFilter)` method, but its `AbstractCompactionFilter` Java class requires a native handle (raw `rocksdb::CompactionFilter*` pointer) passed to its constructor. The Java `filter()` method callback goes through a C++ trampoline that RocksDB's JNI layer manages internally — you can only subclass it from within the same JNI compilation unit.

To implement a custom compaction filter outside the `rocksdbjni` build, we create a standalone C++ shared library that:
- Directly subclasses `rocksdb::CompactionFilter` in C++
- Exposes JNI methods to create/destroy filter instances and update the `minPhyOffset` threshold
- Returns the raw `CompactionFilter*` pointer as a `jlong` to Java

### Architecture

```
┌──────────────────────────────────────────────────────┐
│  ConsumeQueueRocksDBStorage (Java)                   │
│  - CqCompactionFilterJni.createAndSetFilter(cqCfOpts)│
│  - CqCompactionFilterJni.setMinPhyOffset(offset)     │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│  CqCompactionFilterJni.java                          │
│  - Extracts both .so files to a shared temp dir      │
│  - Uses reflection to call setCompactionFilterHandle │
│    (ColumnFamilyOptions private method)              │
│  - Calls native createNativeFilter0() → raw pointer  │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│  libcq_compaction_filter.so (native shim)            │
│                                                      │
│  class CqCompactionFilter                            │
│    : public rocksdb::CompactionFilter { ... }        │
│                                                      │
│  JNI: createNativeFilter0() → new CqCompactionFilter │
│  JNI: setMinPhyOffset0(ptr, offset)                  │
│  JNI: destroyNativeFilter0(ptr) → delete filter      │
│                                                      │
│  NEEDED: librocksdbjni-linux64.so ($ORIGIN RPATH)   │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│  librocksdbjni-linux64.so (official rocksdbjni)      │
│  - All RocksDB C++ classes (CompactionFilter, etc.)  │
│  - JNI glue for all Java↔C++ bindings                │
│  - Compiled with -fno-rtti -D_GLIBCXX_USE_CXX11_ABI=0│
└──────────────────────────────────────────────────────┘
```

### Key design decisions

**1. Direct C++ subclassing with explicit linking**

The shim directly subclasses `rocksdb::CompactionFilter` in C++ and is compiled with matching ABI flags (`-fno-rtti -D_GLIBCXX_USE_CXX11_ABI=0`) to match how `librocksdbjni` was built. It is explicitly linked against `librocksdbjni-linux64.so` (extracted from the `rocksdbjni` jar) with `$ORIGIN` RPATH so the dynamic linker resolves symbols from the same directory.

This replaced an earlier dlopen/RTLD_GLOBAL approach that caused C++ `double free` crashes — loading the same `.so` twice (once via JVM's `RTLD_LOCAL` and once via `RTLD_GLOBAL`) creates conflicting C++ global state (memory allocators, static singletons, vtables).

**2. Raw pointer as jlong, no Java wrapper disposal**

The native shim creates `new CqCompactionFilter()` and returns the raw C++ pointer as a `jlong`. Instead of wrapping it in a Java `AbstractCompactionFilter` subclass (which would try to `dispose()` the native pointer), we use reflection to call `ColumnFamilyOptions.setCompactionFilterHandle(nativeHandle, filterPointer)` directly. This bypasses the Java wrapper lifecycle entirely — the native filter's lifetime is managed by the `ColumnFamilyOptions` and RocksDB.

**3. Shared temp directory for both .so files**

At runtime, `CqCompactionFilterJni` extracts both `librocksdbjni-linux64.so` and `libcq_compaction_filter.so` to the same temp directory, so the `$ORIGIN` RPATH in the shim correctly resolves its `NEEDED` dependency. If the JVM has already loaded `librocksdbjni` (which is the normal case), the shim is extracted to the JVM's existing temp directory alongside the already-loaded library.

**4. Thread-safe minPhyOffset with pthread mutex**

The `CqCompactionFilter` uses a `pthread_mutex_t` to protect concurrent reads of `min_phy_offset_` during compaction (which runs on background threads) and updates from the Java side via `setMinPhyOffset()`. The `volatile` qualifier on `min_phy_offset_` ensures visibility without additional memory barriers.

## Changed files

| File | Change |
|------|--------|
| `pom.xml` | `rocksdb.version` → `rocksdbjni.version=8.4.4`; dependency changed to `org.rocksdb:rocksdbjni` |
| `common/pom.xml` | `rocketmq-rocksdb` → `org.rocksdb:rocksdbjni` |
| `store/.../rocksdb/ConsumeQueueCompactionFilterFactory.java` | **Deleted** — replaced by native shim |
| `store/.../rocksdb/ConsumeQueueRocksDBStorage.java` | Use `CqCompactionFilterJni.createAndSetFilter()` instead of `CompactionFilterFactory`; added `triggerCompactionSync()` and `countEntries()` helpers |
| `store/.../rocksdb/RocksDBOptionsFactory.java` | Remove `setCompactionFilterFactory()` call from `createCQCFOptions()` |
| `store/.../rocksdb/CqCompactionFilterJni.java` | **Rewritten** — uses raw JNI pointer + reflection to set filter on ColumnFamilyOptions |
| `store/.../resources/native/cq_compaction_filter.cpp` | **Rewritten** — direct C++ subclassing, explicit linking |
| `store/.../resources/native/libcq_compaction_filter.so` | **Pre-compiled** native library (Linux x86_64) |
| `store/.../resources/native/librocksdbjni-linux64.so` | **Bundled** from rocksdbjni:8.4.4 jar (Linux x86_64) |
| `store/.../rocksdb/ConsumeQueueRocksDBStorageCompactionTest.java` | **New** — integration test for compaction filter |

## Building the native shim

Prerequisites: `g++` / `clang++`, RocksDB C++ headers matching `rocksdbjni` version (8.4.4), JNI headers from your JDK.

### Linux (x86_64)

```bash
# 1. Extract librocksdbjni from the rocksdbjni jar
ROCKSDB_JAR=~/.m2/repository/org/rocksdb/rocksdbjni/8.4.4/rocksdbjni-8.4.4.jar
unzip -j "$ROCKSDB_JAR" librocksdbjni-linux64.so -d /tmp/rocksdb-native/

# 2. Download matching RocksDB headers
wget https://github.com/facebook/rocksdb/archive/refs/tags/v8.4.4.tar.gz
tar xzf v8.4.4.tar.gz rocksdb-8.4.4/include --strip-components=1

# 3. Compile the shim with explicit linking
export JAVA_HOME=/usr/lib/jvm/java-8   # or your JDK path
g++ -shared -fPIC -O2 -std=c++17 -fno-rtti -D_GLIBCXX_USE_CXX11_ABI=0 \
    -I./include \
    -I${JAVA_HOME}/include \
    -I${JAVA_HOME}/include/linux \
    -Wl,--no-undefined \
    -Wl,-rpath,\$ORIGIN \
    -L/tmp/rocksdb-native \
    -l:librocksdbjni-linux64.so \
    -o libcq_compaction_filter.so \
    store/src/main/resources/native/cq_compaction_filter.cpp

# 4. Verify NEEDED and RPATH
readelf -d libcq_compaction_filter.so | grep -E "NEEDED|RPATH"
# Should show: NEEDED librocksdbjni-linux64.so, RPATH $ORIGIN

# 5. Replace the pre-built .so
cp libcq_compaction_filter.so store/src/main/resources/native/
```

### macOS (arm64 / x86_64)

On macOS, the rocksdbjni jar provides `librocksdbjni-osx.jar` (or platform-specific entries). The approach is similar to Linux with a few differences:

```bash
# 1. Extract the macOS native library from rocksdbjni jar
#    The jar contains librocksdbjni-osx.aarch64 (arm64) or librocksdbjni-osx-x86_64
ROCKSDB_JAR=~/.m2/repository/org/rocksdb/rocksdbjni/8.4.4/rocksdbjni-8.4.4.jar

# For Apple Silicon (arm64):
unzip -j "$ROCKSDB_JAR" librocksdbjni-osx-aarch64 -d /tmp/rocksdb-native/
cp /tmp/rocksdb-native/librocksdbjni-osx-aarch64 /tmp/rocksdb-native/librocksdbjni-osx.dylib

# For Intel Mac (x86_64):
unzip -j "$ROCKSDB_JAR" librocksdbjni-osx-x86_64 -d /tmp/rocksdb-native/
cp /tmp/rocksdb-native/librocksdbjni-osx-x86_64 /tmp/rocksdb-native/librocksdbjni-osx.dylib

# 2. Download matching RocksDB headers
curl -LO https://github.com/facebook/rocksdb/archive/refs/tags/v8.4.4.tar.gz
tar xzf v8.4.4.tar.gz rocksdb-8.4.4/include --strip-components=1

# 3. Compile the shim
export JAVA_HOME=$(/usr/libexec/java_home)
clang++ -shared -fPIC -O2 -std=c++17 -fno-rtti \
    -I./include \
    -I${JAVA_HOME}/include \
    -I${JAVA_HOME}/include/darwin \
    -Wl,-undefined,error \
    -Wl,-rpath,@loader_path \
    -L/tmp/rocksdb-native \
    -l:librocksdbjni-osx.dylib \
    -o libcq_compaction_filter.dylib \
    store/src/main/resources/native/cq_compaction_filter.cpp

# 4. Verify dependencies
otool -L libcq_compaction_filter.dylib
# Should show @loader_path/librocksdbjni-osx.dylib

# 5. Place the output
cp libcq_compaction_filter.dylib store/src/main/resources/native/
```

**Note:** The current C++ source uses `pthread_mutex_t` which is available natively on macOS (POSIX threads are part of the system). No code changes are needed.

### Windows (x86_64)

Windows requires code changes because the current shim uses POSIX-specific APIs (`pthread_mutex_t`). Two options:

**Option A: Use MSYS2/MinGW-w64 with pthreads**

MinGW-w64 provides POSIX thread support (`-lpthread`), so the existing C++ source can compile without modification.

```powershell
# 1. Install MSYS2 from https://www.msys2.org/
# 2. Open MSYS2 UCRT64 shell and install toolchain:
#    pacman -S mingw-w64-ucrt-x86_64-gcc

# 3. Extract the Windows native library from rocksdbjni jar
ROCKSDB_JAR="$HOME/.m2/repository/org/rocksdb/rocksdbjni/8.4.4/rocksdbjni-8.4.4.jar"
unzip -j "$ROCKSDB_JAR" librocksdbjni-win64.dll -d /tmp/rocksdb-native/

# 4. Download matching RocksDB headers
curl -LO https://github.com/facebook/rocksdb/archive/refs/tags/v8.4.4.tar.gz
tar xzf v8.4.4.tar.gz rocksdb-8.4.4/include --strip-components=1

# 5. Compile with MinGW-w64 g++
export JAVA_HOME="/c/Program Files/Java/jdk-8"
x86_64-w64-mingw32-g++ -shared -fPIC -O2 -std=c++17 -fno-rtti -D_GLIBCXX_USE_CXX11_ABI=0 \
    -I./include \
    -I${JAVA_HOME}/include \
    -I${JAVA_HOME}/include/win32 \
    -Wl,--no-undefined \
    -o cq_compaction_filter.dll \
    -L/tmp/rocksdb-native \
    -l:librocksdbjni-win64.dll \
    -lpthread \
    store/src/main/resources/native/cq_compaction_filter.cpp

# 6. Verify dependencies
objdump -p cq_compaction_filter.dll | grep "DLL Name"

# 7. Place the output
cp cq_compaction_filter.dll store/src/main/resources/native/
```

**Option B: Native MSVC build (requires code changes)**

Replace `pthread_mutex_t` with `std::mutex` (C++11 standard library, works on MSVC):

```cpp
// In cq_compaction_filter.cpp, replace:
// #include <pthread.h>
// mutable pthread_mutex_t mutex_;
// With:
#include <mutex>
mutable std::mutex mutex_;

// Replace pthread_mutex_lock/unlock with std::lock_guard:
std::lock_guard<std::mutex> lock(mutex_);
```

Then compile with MSVC:

```powershell
# 1. Open "x64 Native Tools Command Prompt for VS 2022"
# 2. Set paths
set ROCKSDB_INCLUDE=C:\path\to\rocksdb-8.4.4\include
set JAVA_INCLUDE=C:\Program Files\Java\jdk-8\include
set JAVA_INCLUDE_WIN=%JAVA_INCLUDE%\win32
set ROCKSDB_LIB=C:\path\to\rocksdb-native

# 3. Compile (MSVC cl.exe)
cl.exe /std:c++17 /O2 /MD /EHsc /LD ^
    /I"%ROCKSDB_INCLUDE%" ^
    /I"%JAVA_INCLUDE%" ^
    /I"%JAVA_INCLUDE_WIN%" ^
    /link ^
    /LIBPATH:"%ROCKSDB_LIB%" ^
    librocksdbjni-win64.lib ^
    /OUT:cq_compaction_filter.dll ^
    cq_compaction_filter.cpp
```

**Option C: Run on WSL (recommended for development)**

Run the entire RocketMQ build and test under WSL (Windows Subsystem for Linux). This uses the native Linux toolchain and pre-built `.so` with zero code changes:

```bash
# In WSL (Ubuntu)
java -version    # should show WSL JDK
mvn test -pl store -Dtest=ConsumeQueueRocksDBStorageCompactionTest -Djacoco.skip=true
```

## Platform support

The repository ships pre-built native libraries for the following platforms:

| Platform | Library name | Architecture | Status |
|----------|-------------|--------------|--------|
| Linux (glibc) | `libcq_compaction_filter.so` | x86_64 | Pre-built |
| Linux (glibc) | `libcq_compaction_filter.so` | aarch64 | Requires rebuild |
| macOS | `libcq_compaction_filter.dylib` | arm64 | Requires rebuild |
| macOS | `libcq_compaction_filter.dylib` | x86_64 | Requires rebuild |
| Windows | `cq_compaction_filter.dll` | x86_64 | Requires rebuild (Option A/B) |

For platforms without a pre-built library, follow the build instructions above. The `CqCompactionFilterJni.java` already handles platform detection via `System.mapLibraryName()` — just place the correct library in `store/src/main/resources/native/`.

## Limitations

1. **Jacoco incompatibility** — The jacoco Java agent can cause native crashes when combined with dynamically loaded native libraries. Unit tests should be run with `-Djacoco.skip=true` when testing RocksDB functionality.

2. **Single native filter per ColumnFamilyOptions** — Each `ColumnFamilyOptions` instance gets its own `CqCompactionFilter` instance with its own `min_phy_offset_`. Multiple `ConsumeQueueRocksDBStorage` instances (e.g., different topics) each have independent filter thresholds.

3. **Source uses POSIX pthreads** — The C++ source uses `pthread_mutex_t` which is available on Linux and macOS natively. Windows builds require either MinGW-w64 (which provides pthreads) or code changes to use `std::mutex`.

4. **Bundled librocksdbjni size** — The `librocksdbjni-linux64.so` is ~13 MB. It is bundled to ensure the shim can resolve its `NEEDED` dependency without requiring users to manually configure `LD_LIBRARY_PATH`.
