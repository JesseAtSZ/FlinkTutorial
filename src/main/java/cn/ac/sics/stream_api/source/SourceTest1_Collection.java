package cn.ac.sics.stream_api.source;

import cn.ac.sics.stream_api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
  public static void main(String[] args) throws Exception {
    // 创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 从集合中读取数据
    DataStreamSource<SensorReading> dataStream =
        env.fromCollection(
            Arrays.asList(
                new SensorReading("sensor_1", 1547718201L, 1.1),
                new SensorReading("sensor_1", 1547718201L, 1.1),
                new SensorReading("sensor_1", 1547718201L, 1.1),
                new SensorReading("sensor_1", 1547718201L, 1.1)));

    DataStream<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // 打印输出
    dataStream.print("dataStream");

    integerDataStream.print("int");

    env.execute();
  }
}
