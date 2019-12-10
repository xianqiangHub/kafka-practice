package com.bigdata.adminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.Properties;

/**
 * 可以用于管理broker,topic,acl，替代替代了ZKUtils
 */
public class KAClient {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);
//        kafkaAdminClient.
//        创建Topic：createTopics(Collection<NewTopic> newTopics)
//        删除Topic：deleteTopics(Collection<String> topics)
//        显示所有Topic：listTopics()
//        查询Topic：describeTopics(Collection<String> topicNames)
//        查询集群信息：describeCluster()
//        查询ACL信息：describeAcls(AclBindingFilter filter)   //bin/kafka-acls.sh
//        创建ACL信息：createAcls(Collection<AclBinding> acls)
//        删除ACL信息：deleteAcls(Collection<AclBindingFilter> filters)
//        查询配置信息：describeConfigs(Collection<ConfigResource> resources)
//        修改配置信息：alterConfigs(Map<ConfigResource, Config> configs)
//        修改副本的日志目录：alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment)
//        查询节点的日志目录信息：describeLogDirs(Collection<Integer> brokers)
//        查询副本的日志目录信息：describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas)
//        增加分区：createPartitions(Map<String, NewPartitions> newPartitions)
    }

    //	基本的一些操作 可以做成方法
    public static void createTopic() throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);
        NewTopic newTopic = new NewTopic("topic_1", 1, (short) 1);
        CreateTopicsResult createTopicsResult = kafkaAdminClient.createTopics(Arrays.asList(newTopic));
        createTopicsResult.all().get();// 阻塞线程，直到所有主题创建成功
    }

    public static void deleteTopic() throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);
        DeleteTopicsResult deleteTopicsResult = kafkaAdminClient.deleteTopics(Arrays.asList("demo1", "demo2", "demo3", "test"));
        deleteTopicsResult.all().get(); // 阻塞线程，直到所有主题删除成功
    }

    public static void listTopics() throws InterruptedException, ExecutionException {

        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);
        ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        for (TopicListing topicListing : topicListings) {
            System.out.println("主题名称:" + topicListing.name() + " 是否是内部的:" + topicListing.isInternal());
        }
    }

    public static void addPartition() throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);

        /**
         * 把指定的partition数量增加到5个
         */
        // NewPartitions newPartitions = NewPartitions.increaseTo(5); // 参数表示修改后的数量

        /**
         * 把指定的parition数量增加到6个
         * 并且重新指定其副本的分配方案
         */
        List<List<Integer>> newAssignments = new ArrayList<>();
        newAssignments.add(Arrays.asList(1, 2));        // 第1个分区,有两个副本,在broker 1 和 2上	leader节点在broker2上
        newAssignments.add(Arrays.asList(2, 3));        // 第2个分区,有两个副本,在broker 2 和 3上	leader节点在broker3上
        newAssignments.add(Arrays.asList(3, 1));        // 第3个分区,有两个副本,在broker 3 和 1上	leader节点在broker1上
        NewPartitions newPartitions = NewPartitions.increaseTo(6, newAssignments);

        // 执行修改操作
        CreatePartitionsResult createPartitionsResult = kafkaAdminClient.createPartitions(Collections.singletonMap("topic_1", newPartitions));
        createPartitionsResult.all().get();
    }

    public static void alterConfig() throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);
        // ConfigEntry 表示一个配置项
        ConfigEntry configEntry = new ConfigEntry("cleanup.policy", "compact");
        // 多个配置项构造成 Config
        Config config = new Config(Arrays.asList(configEntry));
        // 根据 topic/broker 和Config 构建map
        Map<ConfigResource, Config> configMap = Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "topic_1"), config);
        // 执行修改
        AlterConfigsResult alterConfigsResult = kafkaAdminClient.alterConfigs(configMap);
        alterConfigsResult.all().get(); // 阻塞，直到所有的修改都完成
    }

    public static void describe() throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);
        DescribeConfigsResult describeConfigsResult = kafkaAdminClient.describeConfigs(Arrays.asList(new ConfigResource(ConfigResource.Type.TOPIC, "topic_1")));
        Map<ConfigResource, Config> configMap = describeConfigsResult.all().get();
        for (Map.Entry<ConfigResource, Config> entry : configMap.entrySet()) {
            // 类型和名称
            System.out.println("type=" + entry.getKey().type() + " name=" + entry.getKey().name());
            // 配置详情
            System.out.println(entry.getValue());
        }
    }
}
