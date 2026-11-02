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
package org.apache.rocketmq.store;

import java.io.File;

import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.prewrite.Ext4Prewrite;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gate that decides whether CommitLog prewrite may be activated.
 *
 * <p>Prewrite allocates extents without zeroing them, so a new file exposes whatever its blocks
 * previously held. Recovery can only tell that apart from real data by comparing each message's
 * embedded offset with the position it was read from, which is what
 * {@code checkCommitLogOffsetOnRecover} turns on. These tests pin that coupling: the optimization must
 * never be reachable without the check, independently of whether the native library is present.
 *
 * <p>See {@link CommitLogStaleDataRecoveryTest} for the recovery behaviour the gate depends on.
 */
public class AllocateMappedFilePrewriteTest {

    private static MessageStoreConfig config(boolean prewrite, boolean checkOffset) {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setEnableCommitLogPrewrite(prewrite);
        config.setCheckCommitLogOffsetOnRecover(checkOffset);
        return config;
    }

    @Test
    public void gateRejectsWhenOffsetCheckDisabledTest() {
        assertThat(AllocateMappedFileService.prewriteConfigProblem(config(true, false)))
            .as("prewrite must stay off without the offset check, or stale data from an older "
                + "generation would be accepted as this generation's messages")
            .isNotNull()
            .contains("checkCommitLogOffsetOnRecover");
    }

    @Test
    public void gateAllowsWhenBothEnabledTest() {
        assertThat(AllocateMappedFileService.prewriteConfigProblem(config(true, true)))
            .as("with both switches on the config gate must not block; usability is then decided "
                + "by the native probe")
            .isNull();
    }

    @Test
    public void gateRejectsWhenPrewriteDisabledTest() {
        assertThat(AllocateMappedFileService.prewriteConfigProblem(config(false, true))).isNotNull();
        assertThat(AllocateMappedFileService.prewriteConfigProblem(config(false, false))).isNotNull();
    }

    @Test
    public void offsetCheckDefaultsOffSoPrewriteDefaultsInertTest() {
        MessageStoreConfig defaults = new MessageStoreConfig();
        assertThat(defaults.isEnableCommitLogPrewrite()).isFalse();
        assertThat(defaults.isCheckCommitLogOffsetOnRecover()).isFalse();
        assertThat(AllocateMappedFileService.prewriteConfigProblem(defaults)).isNotNull();
    }

    @Test
    public void prepareIsInertWithoutNativeLibraryTest() {
        // CI has neither the kernel module nor the .so. This pins that a missing library is reported
        // as unsupported rather than silently treated as success.
        if (Ext4Prewrite.isNativeLoaded()) {
            return;
        }
        assertThat(Ext4Prewrite.isSupported()).isFalse();
        assertThat(Ext4Prewrite.prepare("/nonexistent", 4096, Ext4Prewrite.PREPARE_FSYNC_DIR))
            .isEqualTo(Ext4Prewrite.UNSUPPORTED);
        assertThat(Ext4Prewrite.statusText(Ext4Prewrite.UNSUPPORTED)).isNotEmpty();
    }

    @Test
    public void prepareRejectsBadArgumentsTest() {
        if (Ext4Prewrite.isNativeLoaded()) {
            return;
        }
        // Argument validation happens before any native call, so it holds with the library absent too.
        assertThat(Ext4Prewrite.prepare(null, 4096, 0)).isEqualTo(Ext4Prewrite.PRECONDITION);
        assertThat(Ext4Prewrite.prepare("", 4096, 0)).isEqualTo(Ext4Prewrite.PRECONDITION);
        assertThat(Ext4Prewrite.prepare(File.separator + "tmp" + File.separator + "x", 0, 0))
            .isEqualTo(Ext4Prewrite.PRECONDITION);
    }
}
