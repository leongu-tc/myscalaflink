package leongu.myscalaflink.table

import leongu.myscalaflink.util.Utils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2KafkaLocal3 {
  val PRINT_SINK =
    """
      |CREATE TABLE print_table (
      | id VARCHAR,
      | name VARCHAR,
      | gender VARCHAR,
      | age INT,
      | address VARCHAR,
      | log_time TIMESTAMP,
      | row1_time TIMESTAMP
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
      |'properties.group.id' = 'leongu_test3',
      |'scan.startup.mode' = 'latest-offset',
      |'format' = 'json',
      |'json.ignore-parse-errors' = 'false',
      |'json.fail-on-missing-field' = 'true')
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
  val PRINT_SINK2 =
    """
      |CREATE TABLE print_table2 (
      | window_start TIMESTAMP,
      | window_end TIMESTAMP,
      | cnt BIGINT,
      | gender VARCHAR
      | ) WITH ('connector' = 'print')
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

  val PRINT_SQL =
    """
      | INSERT INTO print_table SELECT
      |   *
      |   FROM kafka1
      |""".stripMargin

  val PRINT_SQL2 =
    """
      | INSERT INTO print_table2 SELECT
      |   TUMBLE_START(row1_time, INTERVAL '10' SECOND) as window_start,
      |   TUMBLE_END(row1_time, INTERVAL '10' SECOND) as window_end,
      |   COUNT(id) as cnt,
      |   gender
      |   FROM kafka1
      |   GROUP BY TUMBLE(row1_time, INTERVAL '10' SECOND), gender
      |""".stripMargin

  def main(args: Array[String]) {
//        val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = Utils.localEnv
    env.getConfig.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    tableEnv.executeSql(HBASE_SINK)
    tableEnv.executeSql(KAFKA_SOURCE)
    tableEnv.executeSql(KAFKA_SINK)
    tableEnv.executeSql(PRINT_SINK)
    tableEnv.executeSql(PRINT_SINK2)
    //    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
//    val tableResult1 = tableEnv.executeSql(SQL)
//    val tableResult1 = tableEnv.executeSql(PRINT_SQL)
    val tableResult1 = tableEnv.executeSql(PRINT_SQL2)
  }
}
