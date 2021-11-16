

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}


object KafkaSink extends App {

  val flinkConfig = new Configuration()
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
  env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

  val tEnv = StreamTableEnvironment.create(env)

  //Source - kafka
  val sourceSql: String = "CREATE TABLE orders (`order_id` BIGINT, `customer_name` STRING, `date_of_birth` STRING,  `product` STRING, `order_total_usd` STRING, `town` STRING, " +
    "`country` STRING, `ts` TIMESTAMP(3) METADATA FROM 'timestamp') " + "WITH ('connector' = 'kafka', 'topic' = 'rumble', 'properties.bootstrap.servers' = 'localhost:9092', " +
    "'scan.startup.mode' = 'earliest-offset', 'format' = 'json')"
  tEnv.executeSql(sourceSql)

  tEnv.executeSql("select * from orders limit 100").print()


  //Define Sink kafka
  val sinkSql = "CREATE TABLE transactions (`count` BIGINT, `country` STRING, PRIMARY KEY (country) NOT ENFORCED) WITH " +
    "('connector' = 'upsert-kafka', 'topic' = 'members', 'properties.bootstrap.servers' = 'localhost:9092', 'key.format' = 'json', 'value.format' = 'json')"
  tEnv.executeSql(sinkSql)

  //Send events to sink
  tEnv.executeSql("INSERT INTO transactions SELECT COUNT(order_id) , country from orders group by country").print()

}
