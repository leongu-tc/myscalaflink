package leongu.myscalaflink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2Console2 {
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
      |'topic' = 'kafka_service_check',
      |'properties.bootstrap.servers' = '10.88.100.154:6668,10.88.100.155:6668,10.88.100.156:6668',
      |'properties.group.id' = 'leongu_test',
      |'properties.security.protocol' = 'SASL_SDP',
      |'properties.sasl.mechanism' = 'SDP',
      |'format' = 'csv',
      |'csv.ignore-parse-errors' = 'true',
      |'scan.startup.mode' = 'earliest-offset')
      |""".stripMargin

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//        val env = Utils.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    //    tableEnv.executeSql(HBASE_SINK)
    tableEnv.executeSql(KAFKA_SOURCE)
    tableEnv.executeSql(PRINT_SINK_SQL)
    //    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
    val tableResult1 = tableEnv.executeSql("insert into print_table select msg from kafka1")
  }
}
