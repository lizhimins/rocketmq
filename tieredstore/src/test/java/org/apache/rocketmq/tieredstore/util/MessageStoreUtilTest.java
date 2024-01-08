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
package org.apache.rocketmq.tieredstore.util;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MessageStoreUtilTest {

    @Test
    public void getHashTest() {
        Assert.assertEquals("161c08ff", MessageStoreUtil.getHash("TieredStorageDailyTest"));
    }

    @Test
    public void offset2FileNameTest() {
        Assert.assertEquals("cfcd208400000000000000000000", MessageStoreUtil.offset2FileName(0));
        Assert.assertEquals("b10da56800000000004294937144", MessageStoreUtil.offset2FileName(4294937144L));
    }

    @Test
    public void fileName2OffsetTest() {
        Assert.assertEquals(0, MessageStoreUtil.fileName2Offset("cfcd208400000000000000000000"));
        Assert.assertEquals(4294937144L, MessageStoreUtil.fileName2Offset("b10da56800000000004294937144"));
    }

    @Test
    public void toHumanReadableTest() {
        Map<Long, String> testTable = new HashMap<Long, String>() {
            {
                put(-1L, "-1");
                put(0L, "0Bytes");
                put(1023L, "1023Bytes");
                put(1024L, "1KB");
                put(12_345L, "12.06KB");
                put(10_123_456L, "9.65MB");
                put(10_123_456_798L, "9.43GB");
                put(123 * 1024L * 1024L * 1024L * 1024L, "123TB");
                put(123 * 1024L * 1024L * 1024L * 1024L * 1024L, "123PB");
                put(1_777_777_777_777_777_777L, "1.54EB");
            }
        };
        testTable.forEach((in, expected) ->
            Assert.assertEquals(expected, MessageStoreUtil.toHumanReadable(in)));
    }
}
