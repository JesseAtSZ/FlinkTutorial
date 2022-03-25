package cn.ac.sics.debezium;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DebeziumEngineTest {
  public static void main(String[] args) {
    // Define the configuration for the Debezium Engine with MySQL connector...
    Properties props = new Properties();
    props.setProperty("name", "engine");
    props.setProperty("connector.class", MySqlConnector.class.getCanonicalName());
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
    props.setProperty("offset.storage.file.filename", "src/main/resources/debezium/offset.txt");
    props.setProperty("offset.flush.interval.ms", "60000");
    /* begin connector properties */
    props.setProperty("database.hostname", "127.0.0.1");
    props.setProperty("database.port", "3306");
    props.setProperty("database.user", "root");
    props.setProperty("database.password", "123456");
    props.setProperty("database.include.list", "tpcc");
    props.setProperty("table.include.list", "tpcc.test");
    props.setProperty("snapshot.mode", "initial");
    props.setProperty("provide.transaction.metadata", "true");
    props.setProperty("database.server.name", "my-app-connector");
    props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
    props.setProperty("database.history.file.filename", "src/main/resources/debezium/history.txt");

    // Create the engine with this configuration ...
    try (DebeziumEngine<ChangeEvent<String, String>> engine =
        DebeziumEngine.create(Json.class)
            .using(props)
            .notifying(
                record -> {
                  System.out.println(record);
                })
            .build()) {
      // Run the engine asynchronously ...
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.execute(engine);

      // Do something else or wait for a signal or an event
    } catch (IOException e) {
      e.printStackTrace();
    }

    //    DebeziumEngine<?> engine = DebeziumEngine.create(Connect.class)
    //            .using(props)
    //            .notifying(
    //                    record -> {
    //                      System.out.println(record);
    //                    })
    //            .build();
    //    ExecutorService executor = Executors.newSingleThreadExecutor();
    //    executor.execute(engine);
    // Engine is stopped when the main code is finished
  }
}
