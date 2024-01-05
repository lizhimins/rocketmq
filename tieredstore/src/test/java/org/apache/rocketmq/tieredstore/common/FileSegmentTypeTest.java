package org.apache.rocketmq.tieredstore.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class FileSegmentTypeTest {

    @Test
    public void testGetType() {
        assertEquals(0, FileSegmentType.COMMIT_LOG.getFileType());
        assertEquals(1, FileSegmentType.CONSUME_QUEUE.getFileType());
        assertEquals(2, FileSegmentType.INDEX.getFileType());
    }

    @Test
    public void testFromType() {
        assertEquals(FileSegmentType.COMMIT_LOG, FileSegmentType.valueOf(0));
        assertEquals(FileSegmentType.CONSUME_QUEUE, FileSegmentType.valueOf(1));
        assertEquals(FileSegmentType.INDEX, FileSegmentType.valueOf(2));

        assertThrows(IllegalArgumentException.class, () -> FileSegmentType.valueOf(-1));
        assertThrows(IllegalArgumentException.class, () -> FileSegmentType.valueOf(3));
    }
}