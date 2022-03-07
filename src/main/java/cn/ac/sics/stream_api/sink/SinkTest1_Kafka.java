package cn.ac.sics.stream_api.sink;

import cn.ac.sics.stream_api.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkTest1_Kafka {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    //        // 从文件读取数据
    //        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "consumer-group");
    properties.setProperty(
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty(
        "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("auto.offset.reset", "latest");

    // 从文件读取数据
    DataStream<String> inputStream =
        env.addSource(
            new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

    // 转换成SensorReading类型
    DataStream<String> dataStream =
        inputStream.map(
            line -> {
              String[] fields = line.split(",");
              return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]))
                  .toString();
            });

    dataStream.addSink(
        new FlinkKafkaProducer<String>("localhost:9092", "sinktest", new SimpleStringSchema()));

    env.execute();
  }
}
