package com.bigdata;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import redis.clients.jedis.Jedis;

import java.time.Duration;
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
		props.put("auto.offset.reset", "latest");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// 最开始的订阅列表：atopic、btopic
		consumer.subscribe(Arrays.asList("kafka", "kafka1"));

		while (true) {

			//poll阻塞：一种阻塞时间到循环，一种来一条数据走一次再阻塞
//            ConsumerRecords<String, String> message = consumer.poll(Long.MAX_VALUE);
			ConsumerRecords<String, String> message = consumer.poll(Duration.ofSeconds(1));

			for (final ConsumerRecord<String, String> record : message) {
				System.out.println(record.value());
			}
//ConsumerRecord(topic = kafka, partition = 0, leaderEpoch = 114, offset = 15, CreateTime = 1572921235050, serialized key size = -1, serialized value size = 7, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = wwwwaaa)

			System.out.println("暂停的: " + consumer.paused());

			//******************测试暂停启动*******************************

			Jedis jedis = RedisUtils.getJedis("192.168.1.145");
			TopicPartition topicPartition = new TopicPartition("kafka", 0);
			HashSet<TopicPartition> topicPartitions = new HashSet<>();
			topicPartitions.add(topicPartition);
			//----
			if ("pass".equals(jedis.get("kafka"))) {
				consumer.pause(topicPartitions);
			} else {
				consumer.resume(topicPartitions);
			}

		}

	}
}
