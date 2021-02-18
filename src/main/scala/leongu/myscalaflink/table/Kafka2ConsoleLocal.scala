package leongu.myscalaflink.table

import leongu.myscalaflink.util.Utils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2ConsoleLocal {
  val PRINT_SINK_SQL =
    """
      |CREATE TABLE print_table (
      | msg STRING
      | ) WITH ('connector' = 'print')
      |""".stripMargin
  val KAFKA_SOURCE =
    """
      |CREATE TABLE kafka1 (
      | msg VARCHAR
      | ) WITH (
      |'connector' = 'kafka-0.10',
      |'topic' = 'topic1',
      |'properties.bootstrap.servers' = 'localhost:9092',
      |'properties.group.id' = 'leongu_test',
      |'format' = 'csv',
      |'scan.startup.mode' = 'earliest-offset')
      |""".stripMargin

  def main(args: Array[String]) {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
        val env = Utils.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.executeSql(PRINT_SINK_SQL)
    tableEnv.executeSql(KAFKA_SOURCE)
    //    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
    //    val tableResult1 = tableEnv.executeSql("insert into print_table select text from topic1")
    //    val tableResult1 = tableEnv.executeSql("insert into hdfs_table select text as content, 'a' as dt, 'b' as h from topic1")
    val tableResult1 = tableEnv.executeSql("insert into print_table select msg from kafka1")  }
}
