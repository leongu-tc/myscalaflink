package leongu.myscalaflink.table

import leongu.myscalaflink.util.Utils
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2RedisLocal1 {
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
      | row1_time AS COALESCE(log_time, to_timestamp('1970-01-01 00:00:00.0')),
      | WATERMARK FOR row1_time AS row1_time - INTERVAL '1' SECOND
      | ) WITH (
      |'connector' = 'kafka-0.10',
      |'topic' = 'topic1',
      |'properties.bootstrap.servers' = 'localhost:9092',
      |'properties.group.id' = 'kafka2redis1',
      |'scan.startup.mode' = 'latest-offset',
      |'format' = 'json',
      |'json.ignore-parse-errors' = 'false',
      |'json.fail-on-missing-field' = 'true')
      |""".stripMargin
  val CLUSTERNODES = "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
  val REDIS_SINK =
    s"""
      |CREATE TABLE redis1 (
      | id VARCHAR,
      | name VARCHAR,
      | gender VARCHAR,
      | age INT,
      | address VARCHAR,
      | log_time TIMESTAMP,
      | row1_time TIMESTAMP
      | ) WITH (
      |'connector' = 'redis',
      |'cluster-nodes' = '$CLUSTERNODES',
      |'redis-mode' = 'cluster',
      |'additional-key' = 'redis1',
      |'password' = '1234567',
      |'command' = 'HSET',
      |'maxIdle' = '10',
      |'minIdle' = '1',
      |'partition-column' = 'id')
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
      | INSERT INTO redis1 SELECT
      |   *
      |   FROM kafka1
      |""".stripMargin
  val PRINT_SQL =
    """
      | INSERT INTO print_table SELECT
      |   *
      |   FROM kafka1
      |""".stripMargin


  def main(args: Array[String]) {
//        val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = Utils.localEnv
    env.getCheckpointConfig.setCheckpointInterval(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getConfig.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    //    tableEnv.executeSql(HBASE_SINK)
    tableEnv.executeSql(KAFKA_SOURCE)
    tableEnv.executeSql(REDIS_SINK)
    tableEnv.executeSql(PRINT_SINK)
    tableEnv.executeSql(PRINT_SINK2)
    //    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
    val tableResult1 = tableEnv.executeSql(SQL)
//    val tableResult1 = tableEnv.executeSql(PRINT_SQL)
  }
}
