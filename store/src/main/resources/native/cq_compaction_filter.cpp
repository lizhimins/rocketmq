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

#include <cstdint>
#include <cstring>
#include <pthread.h>
#include <string>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

/* ------------------------------------------------------------------ */
/* Our concrete compaction filter                                     */
/* ------------------------------------------------------------------ */

class CqCompactionFilter : public rocksdb::CompactionFilter {
public:
    CqCompactionFilter() {
        pthread_mutex_init(&mutex_, nullptr);
    }

    ~CqCompactionFilter() override {
        pthread_mutex_destroy(&mutex_);
    }

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
        /* Value[0..7] is phy_offset in big-endian */
        const char* data = existing_value.data();
        long long phy_offset =
            (((long long)data[0]) << 56) |
            (((long long)(unsigned char)data[1]) << 48) |
            (((long long)(unsigned char)data[2]) << 40) |
            (((long long)(unsigned char)data[3]) << 32) |
            (((long long)(unsigned char)data[4]) << 24) |
            (((long long)(unsigned char)data[5]) << 16) |
            (((long long)(unsigned char)data[6]) << 8) |
            (((long long)(unsigned char)data[7]));

        pthread_mutex_lock(&mutex_);
        long long min_offset = min_phy_offset_;
        pthread_mutex_unlock(&mutex_);
        return phy_offset < min_offset;
    }

    void SetMinPhyOffset(long long offset) {
        pthread_mutex_lock(&mutex_);
        min_phy_offset_ = offset;
        pthread_mutex_unlock(&mutex_);
    }

private:
    mutable pthread_mutex_t mutex_;
    volatile long long min_phy_offset_ = 0;
};

/* ------------------------------------------------------------------ */
/* JNI bindings                                                       */
/* ------------------------------------------------------------------ */

#include <jni.h>

/* ------------------------------------------------------------------ */
/* JNI bindings                                                       */
/* ------------------------------------------------------------------ */

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
