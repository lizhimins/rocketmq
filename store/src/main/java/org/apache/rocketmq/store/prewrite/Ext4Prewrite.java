/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.prewrite;

/**
 * JNI binding for the ext4 prewrite native library.
 *
 * <p>Turning a newly allocated CommitLog file's extents into {@code written} state removes the extent
 * tree change that each first write would otherwise cause, and with it the jbd2 transaction that a
 * synchronous flush has to wait for. Measured on a matching ext4 volume: write amplification 6.00x to
 * 1.00x, jbd2 transactions per flush 1 to 0.
 *
 * <p><b>Enabling this makes recovery stricter by necessity.</b> Blocks are allocated without being
 * zeroed, so unwritten areas expose whatever the freed blocks previously held — on a broker's own
 * volume, almost always a CommitLog file that retention just deleted. That content parses as valid
 * messages and passes CRC. Callers must not activate this path unless
 * {@code checkCommitLogOffsetOnRecover} is enabled, which is what rejects a message whose embedded
 * offset does not match the position it was read from.
 *
 * <p>This class deliberately depends on nothing outside the JDK so the JNI symbol names can be
 * verified against it at build time. All logging is done by callers.
 */
public final class Ext4Prewrite {

    /** Extents are now written and verified. */
    public static final int OK = 0;
    /**
     * The kernel module is absent, its ABI does not match, or it was built for a different kernel.
     * The caller must keep the existing allocation path; this is not an error condition.
     */
    public static final int UNSUPPORTED = -1;
    /** A syscall failed. */
    public static final int ERRNO = -2;
    /** Conversion reported success but FIEMAP still shows unwritten extents; the file is not ready. */
    public static final int VERIFY_FAILED = -3;
    /** Preconditions not met (bad path, non-positive size, file shorter than requested). */
    public static final int PRECONDITION = -4;

    /** Extend the file with ftruncate when it is shorter than the requested size. */
    public static final int PREPARE_TRUNCATE = 1;
    /** fsync the parent directory afterwards so the new file's directory entry is durable. */
    public static final int PREPARE_FSYNC_DIR = 2;

    private static final String LIB_NAME = "rocketmq_ext4_prewrite";

    private static boolean nativeLoaded;
    private static String loadFailure;
    private static Integer probeResult;

    private Ext4Prewrite() {
    }

    /**
     * Loads the native library, once. Safe to call repeatedly.
     *
     * @param libPath absolute path to the {@code .so}, or null/empty to resolve via
     *                {@code java.library.path} using the name {@code rocketmq_ext4_prewrite}
     * @return true if the native library is available
     */
    public static synchronized boolean load(String libPath) {
        if (nativeLoaded || loadFailure != null) {
            return nativeLoaded;
        }
        try {
            if (libPath != null && !libPath.trim().isEmpty()) {
                System.load(libPath.trim());
            } else {
                System.loadLibrary(LIB_NAME);
            }
            nativeLoaded = true;
        } catch (Throwable t) {
            loadFailure = t.getClass().getName() + ": " + t.getMessage()
                + (libPath != null && !libPath.trim().isEmpty() ? " (path=" + libPath + ")" : "");
        }
        return nativeLoaded;
    }

    public static boolean isNativeLoaded() {
        return nativeLoaded;
    }

    /** Why {@link #load} failed, or null if it succeeded or was never attempted. */
    public static String getLoadFailure() {
        return loadFailure;
    }

    /**
     * Probes the kernel module, caching only a positive result.
     *
     * <p>Negative results are deliberately not cached: the device node lives in the container's /dev
     * tmpfs and is created after pod start (the host-created misc node does not propagate), so a probe
     * that runs before that setup would otherwise disable the path for the whole process lifetime.
     * Re-probing costs one open + ioctl and only happens until the first success.
     *
     * @return true only if the module is loaded, its ABI matches this binding, and it was built for
     *         the running kernel
     */
    public static boolean isSupported() {
        Integer cached = probeResult;
        if (cached != null && cached == 1) {
            return true;
        }
        if (!nativeLoaded) {
            return false;
        }
        int r;
        try {
            r = nativeProbe();
        } catch (Throwable t) {
            r = 0;
        }
        if (r == 1) {
            probeResult = 1;
            return true;
        }
        return false;
    }

    /** Human readable capability detail, for startup logging. Empty when the native library is absent. */
    public static String getProbeDetail() {
        if (!nativeLoaded) {
            return "native library not loaded: " + loadFailure;
        }
        try {
            return nativeProbeDetail();
        } catch (Throwable t) {
            return "probe failed: " + t;
        }
    }

    /**
     * Makes {@code [0, size)} of the file at {@code path} backed by written extents, without writing
     * any data, then fsyncs the file (and the parent directory if {@link #PREPARE_FSYNC_DIR} is set).
     *
     * <p>A non-zero return means the file did not get the optimization. The file itself is still a
     * valid sparse file, so the caller may continue using it — it simply keeps the old write
     * amplification. Only {@link #VERIFY_FAILED} means the extent state could not be confirmed and
     * should be logged as a warning.
     *
     * @param flags combination of {@link #PREPARE_TRUNCATE} and {@link #PREPARE_FSYNC_DIR}
     * @return one of {@link #OK}, {@link #UNSUPPORTED}, {@link #ERRNO}, {@link #VERIFY_FAILED},
     *         {@link #PRECONDITION}
     */
    public static int prepare(String path, long size, int flags) {
        // Validate before touching native state so a bad argument is reported as such even when the
        // library is absent; otherwise every misuse would look like "unsupported".
        if (path == null || path.isEmpty() || size <= 0) {
            return PRECONDITION;
        }
        if (!nativeLoaded) {
            return UNSUPPORTED;
        }
        try {
            return nativePrepare(path, size, flags);
        } catch (Throwable t) {
            return ERRNO;
        }
    }

    /** Description of a status code returned by {@link #prepare}. */
    public static String statusText(int status) {
        if (!nativeLoaded) {
            return "status " + status + " (native library not loaded)";
        }
        try {
            return nativeStatusText(status);
        } catch (Throwable t) {
            return "status " + status;
        }
    }

    private static native int nativeProbe();

    private static native String nativeProbeDetail();

    private static native int nativePrepare(String path, long size, int flags);

    private static native String nativeStatusText(int status);
}
