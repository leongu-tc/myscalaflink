package leongu.myscalaflink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2Kafka {
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
      | WATERMARK FOR log_time AS log_time - INTERVAL '1' SECOND
      | ) WITH (
      |'connector' = 'kafka-0.10',
      |'topic' = 'topic1',
      |'properties.bootstrap.servers' = '10.88.100.154:6668,10.88.100.155:6668,10.88.100.156:6668',
      |'properties.group.id' = 'leongu_test',
      |'properties.security.protocol' = 'SASL_SDP',
      |'properties.sasl.mechanism' = 'SDP',
      |'properties.kafka.security.authentication.sdp.publickey' = 'yzMVgK2iWzctULWGvrYRSyFYTFVqd6p5ppoo',
      |'properties.kafka.security.authentication.sdp.privatekey' = 'SE1AbMLVg0QUq7cCvlDNvLtC421iKxij',
      |'format' = 'csv',
      |'scan.startup.mode' = 'earliest-offset')
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
      |'properties.bootstrap.servers' = '10.88.100.154:6668,10.88.100.155:6668,10.88.100.156:6668',
      |'properties.group.id' = 'leongu_test',
      |'properties.security.protocol' = 'SASL_SDP',
      |'properties.sasl.mechanism' = 'SDP',
      |'properties.kafka.security.authentication.sdp.publickey' = 'yzMVgK2iWzctULWGvrYRSyFYTFVqd6p5ppoo',
      |'properties.kafka.security.authentication.sdp.privatekey' = 'SE1AbMLVg0QUq7cCvlDNvLtC421iKxij',
      |'format' = 'csv',
      |'csv.ignore-parse-errors' = 'true',
      |'scan.startup.mode' = 'earliest-offset')
      |""".stripMargin

  val SQL =
    """
      | INSERT INTO kafka2 SELECT
      |   TUMBLE_START(log_time, INTERVAL '30' SECOND) as window_start,
      |   TUMBLE_END(log_time, INTERVAL '30' SECOND) as window_end,
      |   COUNT(id) as cnt,
      |   gender
      |   FROM kafka1
      |   GROUP BY TUMBLE(log_time, INTERVAL '30' SECOND), gender
      |""".stripMargin

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//        val env = Utils.localEnv
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
