package cn.ac.sics.word_count;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
  public static void main(String[] args) throws Exception {
    // 创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    //        // 从文件中读取数据
    String inputPath = "src/main/java/cn/ac/sics/word_count/StreamWordCount.java";
    DataStream<String> inputDataStream = env.readTextFile(inputPath);

    // 用 parameter tool 工具从程序启动参数中提取配置项
    //        ParameterTool parameterTool = ParameterTool.fromArgs(args);
    //        String host = parameterTool.get("host");
    //        int port = parameterTool.getInt("port");
    // 从 Socket 文本流读取数据
    //        DataStream<String> inputDataStream = env.socketTextStream(host, port);

    // 基于数据流进行转换计算
    DataStream<Tuple2<String, Integer>> resultStream =
        inputDataStream.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1);

    resultStream.print();

    // 执行任务
    env.execute();
  }
}
