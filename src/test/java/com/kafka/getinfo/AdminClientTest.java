//package com.kafka.getinfo;
//
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//import java.util.concurrent.ExecutionException;
//
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.apache.kafka.clients.admin.Config;
//import org.apache.kafka.clients.admin.ConfigEntry;
//import org.apache.kafka.clients.admin.CreateTopicsResult;
//import org.apache.kafka.clients.admin.DescribeClusterResult;
//import org.apache.kafka.clients.admin.DescribeConfigsResult;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
//import org.apache.kafka.clients.admin.ListTopicsOptions;
//import org.apache.kafka.clients.admin.ListTopicsResult;
//import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.clients.admin.TopicDescription;
//import org.apache.kafka.common.KafkaFuture;
//import org.apache.kafka.common.Node;
//import org.apache.kafka.common.config.ConfigResource;
//
//public class AdminClientTest {
//    private static final String TEST_TOPIC = "hui";
//
//    public static void main(String[] args) throws Exception {
//        Properties props = new Properties();
//        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "h153:9092");
//
//        try (AdminClient client = AdminClient.create(props)) {
//            createTopics(client);
//            describeCluster(client);
//            listAllTopics(client);
//            describeTopics(client);
//            alterConfigs(client);
//            describeConfig(client);
//            deleteTopics(client);
//        }
//    }
//
//    public static void createTopics(AdminClient client) throws ExecutionException, InterruptedException {
//        NewTopic newTopic = new NewTopic(TEST_TOPIC, 1, (short)1);
//        CreateTopicsResult ret = client.createTopics(Arrays.asList(newTopic));
//        ret.all().get();
//        System.out.println("创建成功");
//    }
//
//    public static void describeCluster(AdminClient client) throws ExecutionException, InterruptedException {
//        DescribeClusterResult ret = client.describeCluster();
//        System.out.println(String.format("Cluster id-->%s ; controller-->%s", ret.clusterId().get(), ret.controller().get()));
//        System.out.print("Current cluster nodes info-->");
//        for (Node node : ret.nodes().get()) {
//            System.out.println(node);
//        }
//    }
//
//    public static void listAllTopics(AdminClient client) throws ExecutionException, InterruptedException {
//        ListTopicsOptions options = new ListTopicsOptions();
//        options.listInternal(true); // includes internal topics such as __consumer_offsets
//        ListTopicsResult topics = client.listTopics(options);
//        Set<String> topicNames = topics.names().get();
//        System.out.println("Current topics in this cluster: " + topicNames);
//    }
//
//    public static void describeTopics(AdminClient client) throws ExecutionException, InterruptedException {
//        DescribeTopicsResult ret = client.describeTopics(Arrays.asList(TEST_TOPIC, "__consumer_offsets"));
//        Map<String, TopicDescription> topics = ret.all().get();
//        for (Map.Entry<String, TopicDescription> entry : topics.entrySet()) {
//            System.out.println(entry.getKey() + " ===> " + entry.getValue());
//        }
//    }
//
//    public static void alterConfigs(AdminClient client) throws ExecutionException, InterruptedException {
//        Config topicConfig = new Config(Arrays.asList(new ConfigEntry("cleanup.policy", "compact")));
//        client.alterConfigs(Collections.singletonMap(
//                new ConfigResource(ConfigResource.Type.TOPIC, TEST_TOPIC), topicConfig)).all().get();
//    }
//
//    public static void describeConfig(AdminClient client) throws ExecutionException, InterruptedException {
//        DescribeConfigsResult ret = client.describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, TEST_TOPIC)));
//        Map<ConfigResource, Config> configs = ret.all().get();
//        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
//            ConfigResource key = entry.getKey();
//            Config value = entry.getValue();
//            System.out.println(String.format("Resource type: %s, resource name: %s", key.type(), key.name()));
//            Collection<ConfigEntry> configEntries = value.entries();
//            for (ConfigEntry each : configEntries) {
//                System.out.println(each.name() + " = " + each.value());
//            }
//        }
//    }
//
//    public static void deleteTopics(AdminClient client) throws ExecutionException, InterruptedException {
//        KafkaFuture<Void> futures = client.deleteTopics(Arrays.asList(TEST_TOPIC)).all();
//        futures.get();
//        System.out.println("删除成功");
//    }
//}