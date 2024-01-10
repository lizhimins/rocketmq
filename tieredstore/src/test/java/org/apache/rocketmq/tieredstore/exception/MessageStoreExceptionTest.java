package org.apache.rocketmq.tieredstore.exception;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessageStoreExceptionTest {

    @Test
    public void testMessageStoreException() {
        long position = 100L;
        String requestId = "requestId";
        String error = "ErrorMessage";

        MessageStoreException messageStoreException = new MessageStoreException(MessageStoreErrorCode.IO_ERROR, error);
        Assert.assertEquals(MessageStoreErrorCode.IO_ERROR, messageStoreException.getErrorCode());
        Assert.assertEquals(error, messageStoreException.getMessage());

        messageStoreException.setRequestId(requestId);
        Assert.assertEquals(requestId, messageStoreException.getRequestId());

        messageStoreException.setPosition(position);
        Assert.assertEquals(position, messageStoreException.getPosition());
        Assert.assertNotNull(messageStoreException.toString());
    }
}