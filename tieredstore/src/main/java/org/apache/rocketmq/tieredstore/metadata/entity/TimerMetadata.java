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
package org.apache.rocketmq.tieredstore.metadata.entity;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.Objects;

public class TimerMetadata {

    @JSONField(ordinal = 1)
    private long timestamp;

    @JSONField(ordinal = 2)
    private long minOffset;

    @JSONField(ordinal = 3)
    private long maxOffset;

    @JSONField(ordinal = 4)
    private long consumerOffset;

    @JSONField(ordinal = 5)
    private long updateTimestamp;

    // default constructor is used by fastjson
    @SuppressWarnings("unused")
    public TimerMetadata() {
    }

    public TimerMetadata(long timestamp) {
        this(timestamp, 0L, 0L, System.currentTimeMillis());
    }

    public TimerMetadata(long timestamp, long minOffset, long maxOffset, long updateTimestamp) {
        this.timestamp = timestamp;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.updateTimestamp = updateTimestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
        this.updateTimestamp = System.currentTimeMillis();
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
        this.updateTimestamp = System.currentTimeMillis();
    }

    public long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
        this.updateTimestamp = System.currentTimeMillis();
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TimerMetadata that = (TimerMetadata) o;
        return timestamp == that.timestamp && minOffset == that.minOffset
                && maxOffset == that.maxOffset && consumerOffset == that.consumerOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, minOffset, maxOffset, consumerOffset);
    }
}
