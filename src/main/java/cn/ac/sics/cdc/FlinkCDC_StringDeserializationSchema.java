package cn.ac.sics.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_StringDeserializationSchema {
  public static void main(String[] args) throws Exception {
    // 1.获取 FlinK 执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 开启 CheckPoint
    env.enableCheckpointing(5000);
    env.getCheckpointConfig().setCheckpointTimeout(10000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    //    env.setStateBackend(new FsStateBackend("file:\\src\\main\\resources"));

    // 2.通过Flink CDC 构建 SourceFunction
    DebeziumSourceFunction<String> sourceFunction =
        MySqlSource.<String>builder()
            .hostname("192.168.137.200")
            .port(3306)
            .username("root")
            .password("141164")
            .databaseList("tpcc")
            .tableList("tpcc.source")
            .deserializer(new StringDebeziumDeserializationSchema())
            .startupOptions(StartupOptions.initial())
            .build();

    DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

    // 3.数据打印
    dataStreamSource.print();

    // 4.启动任务
    env.execute("FlinkCDC");
  }
}
