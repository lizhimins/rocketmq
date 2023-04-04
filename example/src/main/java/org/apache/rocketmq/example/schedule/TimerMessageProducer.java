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
package org.apache.rocketmq.example.schedule;

import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.Message;

public class TimerMessageProducer {

    //Note: TimerMessage is a new feature in version 5.0, so be sure to upgrade RocketMQ to version 5.0+ before using it.

    public static final String PRODUCER_GROUP = "TimerMessageProducerGroup";
    public static final String DEFAULT_NAMESERVER_ADDRESS = "11.165.57.3:9876";
    public static final String TOPIC = "timer-topic";

    public static void main(String[] args) throws Exception {
        // Instantiate a producer to send scheduled messages
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        producer.setNamesrvAddr(DEFAULT_NAMESERVER_ADDRESS);

        // Launch producer
        producer.start();
        int totalMessagesToSend = 4;
        List<Pair<Long, SendResult>> resultList = Lists.newArrayList();

        long deliverMs = System.currentTimeMillis() + 10_000L;
        long deliverMs2h = System.currentTimeMillis() + Duration.ofHours(3).toMillis();

        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message(TOPIC, ("123" + i).getBytes(StandardCharsets.UTF_8));
            message.setDeliverTimeMs(deliverMs2h);
            SendResult result = producer.send(message);
            resultList.add(new Pair<>(deliverMs, result));
            System.out.printf("%d %s%n", deliverMs, result);

            //message = new Message(TOPIC, ("234" + i).getBytes(StandardCharsets.UTF_8));
            //message.setDeliverTimeMs(deliverMs2h);
            //result = producer.send(message);
            //resultList.add(new Pair<>(deliverMs2h, result));
            //System.out.printf("%d %s%n", deliverMs2h, result);
            TimeUnit.SECONDS.sleep(2);
        }

        //for (int i = 0; i < 100; i++) {
        //    Pair<Long, SendResult> pair = resultList.get(i);
        //    Message message = new Message(TOPIC, "123".getBytes(StandardCharsets.UTF_8));
        //
        //    // set two props
        //    message.setDeliverTimeMs(pair.getObject1());
        //    MessageAccessor.putProperty(message, MessageConst.PROPERTY_TIMER_DEL_UNIQKEY, pair.getObject2().getMsgId());
        //    SendResult result = producer.send(message);
        //    System.out.printf("%s%n", result);
        //}

        //Message message = new Message(TOPIC, "123".getBytes());
        //
        //// set two props
        //message.setDeliverTimeMs(deliverMs);
        //MessageAccessor.putProperty(message, MessageConst.PROPERTY_TIMER_DEL_UNIQKEY, "messageId");
        //SendResult result = producer.send(message);
        //System.out.printf("%s%n", result);

        // Shutdown producer after use.
        producer.shutdown();
    }
}
