package xyz.jianzha.kafkaStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @author Y_Kevin
 * @date 2020-10-21 18:44
 */
public class Application {
    public static void main(String[] args) {
        String brokers = "hadoop101:9092";
        String zookeepers = "hadoop101:2181";

        // 定义输入和输出的topic
        String from = "log";
        String to = "recommender";

        // 定义kafka Stream 配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        // 创建kafka stream 配置对象
        StreamsConfig config = new StreamsConfig(settings);

        // 定义拓扑构建器
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        // 创建Kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();
        System.out.println("kafka stream started!");
    }
}
