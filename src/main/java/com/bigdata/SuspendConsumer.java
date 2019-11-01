package com.bigdata;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

//1、查看poll阻塞时间
//2、暂停和启动

public class SuspendConsumer {

    public static void main(String[] args) {

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

        while (true) {

            ConsumerRecords<String, String> message = consumer.poll(Long.MAX_VALUE);

//            TopicPartition topicPartition = new TopicPartition();
            HashSet<TopicPartition> topicPartitions = new HashSet<>();

            //
            while (true) {
                //这个poll，拿到数据马上返回，或者阻塞到超时时间返回空数据
                //????
//                ConsumerRecords<String, String> message = consumer.poll();


                //动态控制消费流量
                consumer.pause(topicPartitions);
                Set<TopicPartition> pausedTopicPartitions = consumer.paused();
                consumer.resume(topicPartitions);

            }

        }

    }
}
