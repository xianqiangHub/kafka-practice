package com.bigdata.ordersend;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 在producter端有序发送，没有保证一次语义，失败重试可能会重复发送
 * <p>
 * https://www.cnblogs.com/technologykai/articles/10875480.html
 */
public class ProducterOrderSend {

    public static void main(String[] args) {

        String brokers = "";

        // 创建配置对象
        Properties prop = new Properties();
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        prop.put(ProducerConfig.ACKS_CONFIG, "all"); //保证数据不会丢失
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"); //缓冲区32M  客户端所在机器的本地缓存，满了之后会阻塞生产
        //缓冲区形成一个一个的Batch，每个Batch里包含多条消息，KafkaProducer有一个Sender线程会把多个Batch打包成一个Request发送到Kafka服务器上去
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); //数据满batch后发送  16k
        prop.put(ProducerConfig.LINGER_MS_CONFIG, "0"); //延迟0ms发送   和batch一起使用，当batch一直不满，表示强制发送时间，正常时间大于生产打满batch的时间
        prop.put(ProducerConfig.RETRIES_CONFIG, "5"); //失败尝试次数
//        prop.put("retry.backoff.ms", 500);  //重试间隔时间
//        prop.put("max.request.size", 10485760);  //每次发送给Kafka服务器请求的最大大小
        prop.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");   //避免消息乱序
        //表示发送几条确认一次，ack需要时1或者all
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> sender = new KafkaProducer<>(prop);


    }
}
