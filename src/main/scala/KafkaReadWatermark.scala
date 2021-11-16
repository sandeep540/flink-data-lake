import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.configuration.Configuration


object KafkaReadWatermark extends App {

  val flinkConfig = new Configuration()
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
  env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
  //env.enableCheckpointing(15000L)
  //env.getCheckpointConfig.setCheckpointStorage("file:///Users/sandeep/csv")


  val tEnv = StreamTableEnvironment.create(env)

  //Source - kafka
  val sourceSql: String = "CREATE TABLE cars (`id` BIGINT, `year` BIGINT, `car` STRING, `ts` TIMESTAMP(3)  METADATA FROM 'timestamp', WATERMARK FOR ts AS ts - INTERVAL '1' SECOND ) " +
   "WITH ('connector' = 'kafka', 'topic' = 'cars', 'properties.bootstrap.servers' = 'localhost:9092', " +
    "'scan.startup.mode' = 'earliest-offset', 'format' = 'json')"
  tEnv.executeSql(sourceSql)

  //tEnv.executeSql("describe cars").print()



  tEnv.executeSql("SELECT window_start, window_end, COUNT(car) FROM TABLE(TUMBLE(TABLE cars, DESCRIPTOR(ts), INTERVAL '5' SECOND)) GROUP BY window_start, window_end").print()

}
