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
package org.apache.rocketmq.store.logfile;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.MappedFileQueue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class CommitLogFileInitializerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        assumeFalse(MixAll.isWindows());
    }

    @Test
    public void nonPageAlignedFileIsCompletelyZeroInitializedTest() throws IOException {
        Path target = temporaryFolder.getRoot().toPath().resolve("commitlog/00000000000000000000");
        int fileSize = 2 * 1024 * 1024 + 123;

        assertTrue(CommitLogFileInitializer.initialize(target, fileSize));

        assertEquals(fileSize, Files.size(target));
        assertArrayEquals(new byte[fileSize], Files.readAllBytes(target));
        assertPreparationDirectoryEmpty(target.getParent());
    }

    @Test
    public void existingFileRetainsBytesAndInodeTest() throws IOException {
        Path target = temporaryFolder.getRoot().toPath().resolve("00000000000000000000");
        byte[] original = {1, 2, 3, 4, 5};
        Files.write(target, original);
        Object inode = Files.readAttributes(target, BasicFileAttributes.class).fileKey();
        assertNotNull(inode);

        assertFalse(CommitLogFileInitializer.initialize(target, 1024 * 1024));

        assertArrayEquals(original, Files.readAllBytes(target));
        assertEquals(inode, Files.readAttributes(target, BasicFileAttributes.class).fileKey());
        assertFalse(Files.exists(target.getParent().resolve(".commitlog-init")));
    }

    @Test
    public void existingDanglingLinkIsNotReplacedTest() throws IOException {
        Path parent = temporaryFolder.getRoot().toPath();
        Path target = parent.resolve("00000000000000000000");
        Path missing = parent.resolve("missing");
        Files.createSymbolicLink(target, missing);

        assertFalse(CommitLogFileInitializer.initialize(target, 4097));

        assertEquals(missing, Files.readSymbolicLink(target));
        assertFalse(Files.exists(missing));
        assertFalse(Files.exists(parent.resolve(".commitlog-init")));
    }

    @Test
    public void concurrentPublicationDoesNotReplaceWinnerTest() throws Exception {
        Path target = temporaryFolder.getRoot().toPath().resolve("00000000000000000000");
        int firstSize = 2 * 1024 * 1024 + 13;
        int secondSize = 3 * 1024 * 1024 + 29;
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Object> publishedInode = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = executor.submit(() -> initializeTogether(target, firstSize, ready, start, publishedInode));
            Future<Boolean> second = executor.submit(() -> initializeTogether(target, secondSize, ready, start, publishedInode));
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            boolean firstPublished = first.get(10, TimeUnit.SECONDS);
            boolean secondPublished = second.get(10, TimeUnit.SECONDS);

            assertTrue(firstPublished ^ secondPublished);
            int expectedSize = firstPublished ? firstSize : secondSize;
            assertEquals(expectedSize, Files.size(target));
            assertArrayEquals(new byte[expectedSize], Files.readAllBytes(target));
            assertNotNull(publishedInode.get());
            assertEquals(publishedInode.get(), Files.readAttributes(target, BasicFileAttributes.class).fileKey());
            assertPreparationDirectoryEmpty(target.getParent());
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void invalidFileSizeIsRejectedTest() {
        Path target = temporaryFolder.getRoot().toPath().resolve("00000000000000000000");

        assertThrows(IllegalArgumentException.class, () -> CommitLogFileInitializer.initialize(target, 0));
        assertThrows(IllegalArgumentException.class, () -> CommitLogFileInitializer.initialize(target, -1));
        assertFalse(Files.exists(target));
        assertFalse(Files.exists(target.getParent().resolve(".commitlog-init")));
    }

    @Test
    public void unavailableParentDoesNotPublishTest() throws IOException {
        Path parent = temporaryFolder.newFile("not-a-directory").toPath();
        byte[] original = {7, 8, 9};
        Files.write(parent, original);
        Path target = parent.resolve("00000000000000000000");

        assertThrows(IOException.class, () -> CommitLogFileInitializer.initialize(target, 4097));

        assertFalse(Files.exists(target));
        assertArrayEquals(original, Files.readAllBytes(parent));
    }

    @Test
    public void preparationConflictIsNotSwallowedTest() throws IOException {
        Path parent = temporaryFolder.getRoot().toPath();
        byte[] original = {7, 8, 9};
        Path blocker = Files.write(parent.resolve(".commitlog-init"), original);
        Path target = parent.resolve("00000000000000000000");

        assertThrows(FileAlreadyExistsException.class, () -> CommitLogFileInitializer.initialize(target, 4097));

        assertFalse(Files.exists(target));
        assertArrayEquals(original, Files.readAllBytes(blocker));
    }

    @Test
    public void interruptedInitializationDoesNotPublishTest() throws IOException {
        Path target = temporaryFolder.getRoot().toPath().resolve("00000000000000000000");
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedIOException.class, () -> CommitLogFileInitializer.initialize(target, 4097));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertFalse(Files.exists(target));
        assertFalse(Files.exists(target.getParent().resolve(".commitlog-init")));
    }

    @Test
    public void hiddenPreparationFileIsNotLoadedOrDeletedTest() throws IOException {
        Path parent = temporaryFolder.newFolder("commitlog").toPath();
        Path preparationDirectory = Files.createDirectory(parent.resolve(".commitlog-init"));
        byte[] partialBytes = {3, 2, 1};
        Path partialFile = Files.write(preparationDirectory.resolve("unfinished.tmp"), partialBytes);
        Path target = parent.resolve("00000000000000000000");
        int fileSize = 4097;
        assertTrue(CommitLogFileInitializer.initialize(target, fileSize));
        MappedFileQueue queue = new MappedFileQueue(parent.toString(), fileSize, null);
        try {
            assertTrue(queue.load());
            assertEquals(1, queue.getMappedFiles().size());
            assertEquals(target.toString(), queue.getFirstMappedFile().getFileName());
            assertArrayEquals(partialBytes, Files.readAllBytes(partialFile));
            try (Stream<Path> files = Files.list(preparationDirectory)) {
                assertEquals(1, files.count());
            }
        } finally {
            queue.destroy();
        }
    }

    private boolean initializeTogether(Path target, int fileSize, CountDownLatch ready, CountDownLatch start,
        AtomicReference<Object> publishedInode) throws Exception {
        ready.countDown();
        assertTrue(start.await(10, TimeUnit.SECONDS));
        boolean published = CommitLogFileInitializer.initialize(target, fileSize);
        if (published) {
            publishedInode.set(Files.readAttributes(target, BasicFileAttributes.class).fileKey());
        }
        return published;
    }

    private void assertPreparationDirectoryEmpty(Path parent) throws IOException {
        try (Stream<Path> files = Files.list(parent.resolve(".commitlog-init"))) {
            assertEquals(0, files.count());
        }
    }
}
