import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.configuration.Configuration


object MainApp extends App {

  val flinkConfig = new Configuration()
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
  env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

  val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  //Source look up
  val sql: String = "CREATE TABLE lookup (`id` BIGINT, `blacklist` BOOLEAN) WITH ('connector' = 'filesystem', 'path' = 'file:///Users/sandeep/IdeaProjects/flink-data-lake/lookup.csv', 'format' = 'csv')"
  tEnv.executeSql(sql)

  tEnv.executeSql("select * from lookup").print()


}

