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
package org.apache.rocketmq.tieredstore.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InFlightRequestFutureTest {

    @Test
    public void testGetStartOffset() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        long startOffset = inFlightRequestFuture.getStartOffset();
        assertEquals(10, startOffset);
    }

    @Test
    public void testGetFirstFuture() throws ExecutionException, InterruptedException {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        CompletableFuture<Long> firstFuture = inFlightRequestFuture.getFirstFuture();
        assertEquals(new Long(20), firstFuture.get());
    }

    @Test
    public void testGetFuture() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        CompletableFuture<Long> future = inFlightRequestFuture.getFuture(11);
        assertEquals(new Long(-1L), future.join());
    }

    @Test
    public void testGetLastFuture() throws ExecutionException, InterruptedException {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        CompletableFuture<Long> lastFuture = inFlightRequestFuture.getLastFuture();
        assertEquals(new Long(20), lastFuture.get());
    }

    @Test
    public void testIsFirstDone() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        boolean firstDone = inFlightRequestFuture.isFirstDone();
        assertTrue(firstDone);
    }

    @Test
    public void testIsAllDone() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture1 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture2 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture3 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture4 = new CompletableFuture<>();
        completableFuture1.complete(20L);
        completableFuture2.complete(30L);
        completableFuture3.complete(40L);
        futureList.add(Pair.of(1, completableFuture1));
        futureList.add(Pair.of(2, completableFuture2));
        futureList.add(Pair.of(3, completableFuture3));
        futureList.add(Pair.of(4, completableFuture4));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        boolean allDone = inFlightRequestFuture.isAllDone();
        assertFalse(allDone);
    }

    @Test
    public void testGetAllFuture() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture1 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture2 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture3 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture4 = new CompletableFuture<>();
        futureList.add(Pair.of(1, completableFuture1));
        futureList.add(Pair.of(2, completableFuture2));
        futureList.add(Pair.of(3, completableFuture3));
        futureList.add(Pair.of(4, completableFuture4));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        List<CompletableFuture<Long>> allFuture = inFlightRequestFuture.getAllFuture();
        assertEquals(4, allFuture.size());
    }
}