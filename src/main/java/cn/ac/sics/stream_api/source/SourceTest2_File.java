package cn.ac.sics.stream_api.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 从文件读取数据
    DataStream<String> dataStream = env.readTextFile("src/main/resources/sensor.txt");
    dataStream.print();
    env.execute();
  }
}
