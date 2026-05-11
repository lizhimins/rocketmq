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

/*
 * Native compaction filter for ConsumeQueue entries.
 *
 * Subclass rocksdb::CompactionFilter directly, create instances in C++,
 * and pass the raw C++ pointer as a jlong to Java. Java's
 * AbstractCompactionFilter(nativeHandle) wraps it seamlessly.
 *
 * All rocksdb symbols are declared weak so they resolve at runtime to the
 * symbols already loaded by the JVM's ClassLoader.
 */

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

/* ------------------------------------------------------------------ */
/* Our concrete compaction filter                                     */
/* ------------------------------------------------------------------ */

class CqCompactionFilter : public rocksdb::CompactionFilter {
public:
    const char* Name() const override {
        return "ConsumeQueueCompactionFilter";
    }

    bool Filter(int /*level*/, const rocksdb::Slice& /*key*/,
                const rocksdb::Slice& existing_value, std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
        static const int CQ_MIN_SIZE = 28;
        if (existing_value.size() < static_cast<size_t>(CQ_MIN_SIZE)) {
            return false;
        }
        const unsigned char* data =
            reinterpret_cast<const unsigned char*>(existing_value.data());
        int64_t phy_offset =
            (static_cast<int64_t>(data[0]) << 56) |
            (static_cast<int64_t>(data[1]) << 48) |
            (static_cast<int64_t>(data[2]) << 40) |
            (static_cast<int64_t>(data[3]) << 32) |
            (static_cast<int64_t>(data[4]) << 24) |
            (static_cast<int64_t>(data[5]) << 16) |
            (static_cast<int64_t>(data[6]) << 8) |
            (static_cast<int64_t>(data[7]));

        int64_t min_offset = min_phy_offset_.load(std::memory_order_relaxed);
        return phy_offset < min_offset;
    }

    void SetMinPhyOffset(int64_t offset) {
        min_phy_offset_.store(offset, std::memory_order_relaxed);
    }

private:
    std::atomic<int64_t> min_phy_offset_{0};
};

/* ------------------------------------------------------------------ */
/* JNI bindings                                                       */
/* ------------------------------------------------------------------ */

#include <jni.h>

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_apache_rocketmq_store_rocksdb_CqCompactionFilterJni_createNativeFilter0(
    JNIEnv* env, jclass clazz) {
    CqCompactionFilter* filter = new CqCompactionFilter();
    return reinterpret_cast<jlong>(filter);
}

JNIEXPORT void JNICALL
Java_org_apache_rocketmq_store_rocksdb_CqCompactionFilterJni_setMinPhyOffset0(
    JNIEnv* env, jclass clazz, jlong filterPtr, jlong minPhyOffset) {
    CqCompactionFilter* filter = reinterpret_cast<CqCompactionFilter*>(filterPtr);
    filter->SetMinPhyOffset(minPhyOffset);
}

} // extern "C"
