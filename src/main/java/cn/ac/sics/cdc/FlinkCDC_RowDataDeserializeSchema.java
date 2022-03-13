package cn.ac.sics.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.LinkedList;
import java.util.List;

import static java.time.ZoneId.systemDefault;

public class FlinkCDC_RowDataDeserializeSchema {
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
    List<RowType.RowField> fields = new LinkedList<>();
    fields.add(new RowType.RowField("column_1", new IntType(false)));
    fields.add(new RowType.RowField("table", new VarCharType(1000)));

    RowType rowType = new RowType(false, fields);
    InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(rowType);

    RowDataDebeziumDeserializeSchema rowDataDebeziumDeserializeSchema =
        new RowDataDebeziumDeserializeSchema(
            rowType, typeInfo, ((rowData, rowKind) -> {}), systemDefault());

    DebeziumSourceFunction<RowData> sourceFunction =
        MySqlSource.<RowData>builder()
            .hostname("192.168.137.200")
            .port(3306)
            .username("root")
            .password("141164")
            .databaseList("tpcc")
            .tableList("tpcc.source")
            .deserializer(rowDataDebeziumDeserializeSchema)
            .startupOptions(StartupOptions.initial())
            .build();

    DataStreamSource<RowData> dataStreamSource = env.addSource(sourceFunction);

    // 3.数据打印
    dataStreamSource.print();

    // 4.启动任务
    env.execute("FlinkCDC_RowDataDeserializeSchema");
  }
}
