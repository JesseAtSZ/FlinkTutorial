package cn.ac.sics.table_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest5_MysqlSink {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接Kafka，读取数据
        tableEnv
                .connect(
                        new Kafka()
                                .version("0.11")
                                .topic("sensor")
                                .property("zookeeper.connect", "localhost:2181")
                                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        // 3. 查询转换
        // 简单转换
        Table inputTable = tableEnv.from("inputTable");
        Table resultTable =
                tableEnv.sqlQuery("select id, temperature from inputTable where id = 'sensor_1'");

        // 4. 创建Table来描述MySql中的数据，作为输入和输出
        String sinkDDL =
                "create table jdbcOutputTable (" +
                        " id varchar(20) not null, " +
                        " temperature double not null " +
                        ") with (" +
                        " 'connector.type' = 'jdbc', " +
                        " 'connector.url' = 'jdbc:mysql://localhost:3306/test', " +
                        " 'connector.table' = 'sensor_count', " +
                        " 'connector.driver' = 'com.mysql.jdbc.Driver', " +
                        " 'connector.username' = 'root', " +
                        " 'connector.password' = '123456' )";
        tableEnv.sqlUpdate(sinkDDL);    // 执行DDL创建表
        resultTable.insertInto("jdbcOutputTable");

        env.execute();
    }
}
