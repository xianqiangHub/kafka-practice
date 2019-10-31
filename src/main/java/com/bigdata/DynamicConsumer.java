package com.bigdata;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DynamicConsumer {

    public static void main(String[] args) {

        ConcurrentLinkedQueue<String> concurrenQueue = new ConcurrentLinkedQueue<>();
        // [atopic, btopic]
//[atopic, btopic]
//[atopic, btopic]
//[atopic, btopic]
//[atopic, btopic]
//[ctopic, btopic]


        //起一个线程修改 topic
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 变更为订阅topic： btopic， ctopic   在10s之后
                concurrenQueue.addAll(Arrays.asList("btopic", "ctopic"));
            }
        };
        //启动线程
        new Thread(runnable).start();

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put("group.id", "my-group1");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 最开始的订阅列表：atopic、btopic
        consumer.subscribe(Arrays.asList("atopic", "btopic"));

//        Runnable runnable = new Runnable() {
//            @Override
//            public void run() {
//
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                // 在起个线程，直接修改订阅的topic
//                consumer.subscribe(Arrays.asList("btopic", "ctopic"));
////                java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
//            }
//        };
//        //启动线程
//        new Thread(runnable).start();


        //flink为isrunning
        while (true) {
            //阻塞有数据就会拉过来，但是加上时间之后有机会轮询订阅状态是否更新
            consumer.poll(Duration.ofSeconds(2));   //死循环，可以阻塞时间
//            consumer.wakeup();
//            consumer.pause();

            System.out.println(consumer.subscription());

            //10s进入
            if (!concurrenQueue.isEmpty()) {
                Iterator<String> iter = concurrenQueue.iterator();
                List<String> topics = new ArrayList<>();
                while (iter.hasNext()) {
                    topics.add(iter.next());
                }
                concurrenQueue.clear();
                consumer.subscribe(topics); // 重新订阅topic
            }

        }
    }
}