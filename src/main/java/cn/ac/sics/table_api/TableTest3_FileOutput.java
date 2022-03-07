package cn.ac.sics.table_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableTest3_FileOutput {
  public static void main(String[] args) throws Exception {
    // 1. 创建环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 2. 表的创建：连接外部系统，读取数据
    // 读取文件
    String filePath = "src/main/resources/sensor.txt";
    tableEnv
        .connect(new FileSystem().path(filePath))
        .withFormat(new Csv())
        .withSchema(
            new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE()))
        .createTemporaryTable("inputTable");
    Table inputTable = tableEnv.from("inputTable");
    inputTable.printSchema();

    // 3. 简单转换
    Table resultTable =
        tableEnv.sqlQuery("select id, temperature from inputTable where id = 'sensor_1'");

    // 4. 输出到文件
    // 连接外部文件注册输出表
    String outputPath = "src/main/resources/result.txt";
    tableEnv
        .connect(new FileSystem().path(outputPath))
        .withFormat(new Csv())
        .withSchema(
            new Schema().field("id", DataTypes.STRING()).field("temperature", DataTypes.DOUBLE()))
        .createTemporaryTable("outputTable");

    resultTable.executeInsert("outputTable");
  }
}
