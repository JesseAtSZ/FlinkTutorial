package cn.ac.sics.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDCJSON {
  public static void main(String[] args) throws Exception {
    // 1.获取 FlinK 执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 1.1 开启 CheckPoint
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
            .deserializer(new CustomerDeserializationSchema())
            .startupOptions(StartupOptions.initial())
            .build();

    DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

    // 3.数据打印
    dataStreamSource.print();

    // 4.启动任务
    env.execute("FlinkCDC");
  }
}

class CustomerDeserializationSchema implements DebeziumDeserializationSchema {

  @Override
  public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
    // 创建JSON对象用于封装结果数据
    JSONObject result = new JSONObject();

    // 获取库名&表名
    String topic = sourceRecord.topic();
    String[] fields = topic.split("\\.");
    result.put("db", fields[1]);
    result.put("tableName", fields[2]);

    // 获取 before 数据
    Struct value = (Struct) sourceRecord.value();
    Struct before = value.getStruct("before");
    JSONObject beforeJson = new JSONObject();

    if (before != null) {
      Schema schema = before.schema();
      List<Field> fieldList = schema.fields();

      for (Field field : fieldList) {
        beforeJson.put(field.name(), before.get(field));
      }
    }
    result.put("before", beforeJson);

    // 获取 after 数据
    Struct after = value.getStruct("after");
    JSONObject afterJson = new JSONObject();

    if (after != null) {
      Schema schema = after.schema();
      List<Field> fieldList = schema.fields();

      for (Field field : fieldList) {
        afterJson.put(field.name(), after.get(field));
      }
    }
    result.put("after", beforeJson);

    // 获取操作类型
    Envelope.Operation operation = Envelope.operationFor(sourceRecord);
    result.put("op", operation);

    // 输出数据
    collector.collect(result.toString());
  }

  @Override
  public TypeInformation getProducedType() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }
}
