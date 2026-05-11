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
- Exposes JNI methods to create filter instances and update the `minPhyOffset` threshold
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
│  - Extracts libcq_compaction_filter.so to the same   │
│    temp dir as the already-loaded rocksdbjni .so     │
│  - Uses NativeCqCompactionFilter wrapper with        │
│    disOwnNativeHandle() + public setCompactionFilter │
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
│                                                      │
│  NEEDED: librocksdbjni-linux64.so ($ORIGIN RPATH)    │
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

**2. Raw pointer as jlong, wrapped with disOwnNativeHandle()**

The native shim creates `new CqCompactionFilter()` and returns the raw C++ pointer as a `jlong`. A thin Java wrapper `NativeCqCompactionFilter extends AbstractCompactionFilter<Slice>` passes this pointer to the protected `AbstractCompactionFilter(long)` constructor, then calls `disOwnNativeHandle()` so that `close()` does not free the native memory. This is critical because `AbstractRocksDBStorage.shutdown()` closes `ColumnFamilyOptions` (step 2) before closing the DB (step 4) — without `disOwnNativeHandle()`, background compaction threads would access a freed filter. The filter is then set via the public `ColumnFamilyOptions.setCompactionFilter()` API, avoiding reflection and ensuring JDK 17+ compatibility.

**3. Shared temp directory for .so resolution**

At runtime, `CqCompactionFilterJni` loads `librocksdbjni-linux64.so` from the rocksdbjni JAR first (via `System.loadLibrary` or extraction to a temp dir), then extracts `libcq_compaction_filter.so` to the same temp directory. This ensures the `$ORIGIN` RPATH in the shim correctly resolves its `NEEDED` dependency on `librocksdbjni-linux64.so`. The rocksdbjni native library is NOT bundled in the RocketMQ repository — it is sourced from the `org.rocksdb:rocksdbjni:8.4.4` JAR at runtime.

**4. Thread-safe minPhyOffset with std::atomic**

The `CqCompactionFilter` uses `std::atomic<int64_t>` with `memory_order_relaxed` for `min_phy_offset_`. This is sufficient because there is a single writer (Java main thread via JNI) and one reader (compaction background thread), and eventual consistency is acceptable — a slightly stale threshold only means a few extra entries survive one compaction cycle. This replaces the earlier `pthread_mutex` approach, eliminating per-entry lock/unlock overhead during full compaction over hundreds of millions of entries.

## Changed files

| File | Change |
|------|--------|
| `pom.xml` | `rocksdb.version` → `rocksdbjni.version=8.4.4`; dependency changed to `org.rocksdb:rocksdbjni` |
| `common/pom.xml` | `rocketmq-rocksdb` → `org.rocksdb:rocksdbjni` |
| `store/.../rocksdb/ConsumeQueueCompactionFilterFactory.java` | **Deleted** — replaced by native shim |
| `store/.../rocksdb/ConsumeQueueRocksDBStorage.java` | Use `CqCompactionFilterJni.createAndSetFilter()` instead of `CompactionFilterFactory`; added `triggerCompactionSync()` and `countEntries()` helpers |
| `store/.../rocksdb/RocksDBOptionsFactory.java` | Remove `setCompactionFilterFactory()` call from `createCQCFOptions()` |
| `store/.../rocksdb/CqCompactionFilterJni.java` | **Rewritten** — uses raw JNI pointer + `NativeCqCompactionFilter` wrapper via public API |
| `store/.../rocksdb/NativeCqCompactionFilter.java` | **New** — thin `AbstractCompactionFilter<Slice>` wrapper with `disOwnNativeHandle()` |
| `store/.../resources/native/cq_compaction_filter.cpp` | **Rewritten** — direct C++ subclassing, explicit linking |
| `store/.../resources/native/libcq_compaction_filter.so` | **New** — pre-compiled native library (Linux x86_64) |
| `store/.../rocksdb/CqCompactionFilterJniTest.java` | **New** — integration test for compaction filter |

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

**Note:** The C++ source uses `std::atomic<int64_t>` (C++17 standard library) for thread safety. No platform-specific threading APIs are needed.

### Windows (x86_64)

Windows builds work with any C++17 compiler since the source now uses `std::atomic` instead of POSIX pthreads. Two options:

**Option A: Use MSYS2/MinGW-w64**

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

**Option B: Native MSVC build**

No code changes needed — the source uses `std::atomic` which is standard C++17. Compile with MSVC:

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
mvn test -pl store -Dtest=CqCompactionFilterJniTest -Djacoco.skip=true
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

3. **C++17 required** — The C++ source uses `std::atomic<int64_t>` which requires a C++17-capable compiler. All modern compilers (GCC 7+, Clang 5+, MSVC 2017+) support this.

4. **Shim depends on rocksdbjni native library at runtime** — The `libcq_compaction_filter.so` has a `DT_NEEDED` entry for `librocksdbjni-linux64.so` (~13 MB). The `CqCompactionFilterJni` class handles this by extracting the shim to the same temp directory as the rocksdbjni native library, so the `$ORIGIN` RPATH resolves correctly without requiring `LD_LIBRARY_PATH`.
