package leongu.myscalaflink.table

import leongu.myscalaflink.util.Utils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * TODO CSV Format cannot handle dirty data
 */
object Kafka2KafkaLocal2 {
  val PRINT_SINK_SQL =
    """
      |CREATE TABLE print_table (
      | msg STRING
      | ) WITH ('connector' = 'print')
      |""".stripMargin

  val KAFKA_SOURCE =
    """
      |CREATE TABLE kafka1 (
      | id VARCHAR,
      | name VARCHAR,
      | gender VARCHAR,
      | age INT,
      | address VARCHAR,
      | log_time TIMESTAMP,
      | row1_time AS COALESCE(log_time, to_timestamp('1970-01-01 00:00:00:0')),
      | WATERMARK FOR row1_time AS row1_time - INTERVAL '1' SECOND
      | ) WITH (
      |'connector' = 'kafka-0.10',
      |'topic' = 'topic1',
      |'properties.bootstrap.servers' = 'localhost:9092',
      |'properties.group.id' = 'leongu_test2',
      |'scan.startup.mode' = 'earliest-offset',
      |'format' = 'csv',
      |'csv.ignore-parse-errors' = 'true',
      |'csv.allow-comments' = 'true')
      |""".stripMargin
  val KAFKA_SINK =
    """
      |CREATE TABLE kafka2 (
      | window_start TIMESTAMP,
      | window_end TIMESTAMP,
      | cnt BIGINT,
      | gender VARCHAR
      | ) WITH (
      |'connector' = 'kafka-0.10',
      |'topic' = 'topic2',
      |'properties.bootstrap.servers' = 'localhost:9092',
      |'properties.group.id' = 'leongu_test',
      |'scan.startup.mode' = 'earliest-offset',
      |'format' = 'csv',
      |'csv.ignore-parse-errors' = 'true',
      |'csv.allow-comments' = 'true')
      |""".stripMargin

  val SQL =
    """
      | INSERT INTO kafka2 SELECT
      |   TUMBLE_START(row1_time, INTERVAL '30' SECOND) as window_start,
      |   TUMBLE_END(row1_time, INTERVAL '30' SECOND) as window_end,
      |   COUNT(id) as cnt,
      |   gender
      |   FROM kafka1
      |   GROUP BY TUMBLE(row1_time, INTERVAL '30' SECOND), gender
      |""".stripMargin

  def main(args: Array[String]) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val env = Utils.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    //    tableEnv.executeSql(HBASE_SINK)
    tableEnv.executeSql(KAFKA_SOURCE)
    tableEnv.executeSql(KAFKA_SINK)
    //    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
    val tableResult1 = tableEnv.executeSql(SQL)
  }
}
