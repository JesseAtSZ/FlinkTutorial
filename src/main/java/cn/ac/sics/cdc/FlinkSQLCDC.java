package cn.ac.sics.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQLCDC {
  public static void main(String[] args) throws Exception {
    // 1.获取执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 2.使用 Flink SQL DDL模式构建 CDC 表
    tableEnv.executeSql(
        "create table source ("
            + "column_1 int"
            + ") with ("
            + "'connector' = 'mysql-cdc',"
            + "'scan.incremental.snapshot.enabled' = 'false',"
            + "'hostname' = '192.168.137.200',"
            + " 'port' = '3306',"
            + "'username' = 'root',"
            + "'password' = '141164',"
            + "'database-name' = 'tpcc',"
            + "'table-name' = 'source')");

    Table table = tableEnv.sqlQuery("select * from source");
    DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
    retractStream.print();
    //    retractStream.addSink()
    env.execute("FlinkSQLCDC");
  }
}
