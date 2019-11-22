package com.bigdata.transaction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * kafka的事务体现在Kafkastream，consumer-process-producter，消费的数据at exactly once 能打到下一个topic
 * <p>
 * 幂等
 * 是product端到一个分区实现幂等
 * 1、PID（Producer ID），用来标识每个 producer client；
 * 2、sequence numbers，client 发送的每条消息都会带相应的 sequence number，Server 端就是根据这个值来判断数据是否重复
 * <p>
 * 事务：
 * 在product端多分区的原子性写入，多分区原子性写入保证producer发送到多个分区的一批消息要么都成功要么都失败
 * =>所谓的失败是指对事务型consumer不可见
 * 在consumer端分为：read_uncommitted和read_committed，
 * read_committed指的是consumer只能读取已成功提交事务的消息（当然也包括非事务型producer生产的消息）
 */
public class KafkaTransaction {

    private static Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.52.199.51:9092");
        props.put("retries", 3); // 重试次数
        props.put("batch.size", 100); // 批量发送大小
        props.put("buffer.memory", 33554432); // 缓存大小，根据本机内存大小配置
        props.put("linger.ms", 1000); // 发送频率，满足任务一个条件发送
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // producet的事务
        props.put("client.id", "producer-syn-2"); // 发送端id,便于统计
        props.put("transactional.id", "producer-2"); // 每台机器唯一
        props.put("enable.idempotence", true); // 设置幂等性
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        props.put("acks", "all"); // 当 enable.idempotence 为 true，这里默认为 all
        return props;
    }

    private static Properties getConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.52.199.51:9092");
        props.put("group.id", "test_3");
        props.put("session.timeout.ms", 30000);       // 如果其超时，将会可能触发rebalance并认为已经死去，重新选举Leader
        props.put("enable.auto.commit", "false");      // 开启自动提交
        props.put("auto.commit.interval.ms", "1000"); // 自动提交时间
        props.put("auto.offset.reset", "earliest"); // 从最早的offset开始拉取，latest:从最近的offset开始消费
        props.put("max.poll.records", "100"); // 每次批量拉取条数
        props.put("max.poll.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //consumer开启事务
        props.put("client.id", "producer-syn-1"); // 发送端id,便于统计
        props.put("isolation.level", "read_committed"); // 设置隔离级别
        return props;
    }

    public static void main(String[] args) {

        // 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProps());
        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps());
        // 初始化事务
        producer.initTransactions();
        // 订阅主题
        consumer.subscribe(Arrays.asList("consumer-tran"));
        for (; ; ) {
            // 开启事务
            producer.beginTransaction();
            // 接受消息
            ConsumerRecords<String, String> records = consumer.poll(500); //因为一直阻塞到获取元数据，后面设置的超时时间不包括此时间，也就是说当远端broker不可用会一直阻塞
//            poll(Duration)这个版本修改了这样的设计，会把元数据获取也计入整个超时时间
            // 处理逻辑
            try {
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                for (ConsumerRecord record : records) {
                    // 处理消息
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    // 记录提交的偏移量
                    commits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                    // 产生新消息
                    Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>("consumer-send", record.value() + "send"));

//                    // 发送消息到producer-syn
//                    producer.send(new ProducerRecord<String, String>("producer-syn","test3"));
//                    // 发送消息到producer-asyn
//                    Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<String, String>("producer-asyn","test4"));
//
                }
                // 提交偏移量
                producer.sendOffsetsToTransaction(commits, "group0323");
                // 事务提交
                producer.commitTransaction();

            } catch (Exception e) {
                e.printStackTrace();
                producer.abortTransaction();
            }
        }

    }


}
