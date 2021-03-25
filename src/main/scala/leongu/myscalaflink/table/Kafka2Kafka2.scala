package leongu.myscalaflink.table

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2Kafka2 {
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
      |'properties.bootstrap.servers' = '10.88.100.154:6668,10.88.100.155:6668,10.88.100.156:6668',
      |'properties.group.id' = 'leongu_test',
      |'properties.security.protocol' = 'SASL_SDP',
      |'properties.sasl.mechanism' = 'SDP',
      |'properties.kafka.security.authentication.sdp.publickey' = 'yzMVgK2iWzctULWGvrYRSyFYTFVqd6p5ppoo',
      |'properties.kafka.security.authentication.sdp.privatekey' = 'SE1AbMLVg0QUq7cCvlDNvLtC421iKxij',
      |'scan.startup.mode' = 'latest-offset',
      |'format' = 'json',
      |'json.ignore-parse-errors' = 'true',
      |'json.fail-on-missing-field' = 'false')
      |""".stripMargin
  val KAFKA_SINK =
    """
      |CREATE TABLE kafka2 (
      | msg VARCHAR
      | ) WITH (
      |'connector' = 'kafka-0.10',
      |'topic' = 'topic2',
      |'properties.bootstrap.servers' = '10.88.100.154:6668,10.88.100.155:6668,10.88.100.156:6668',
      |'properties.group.id' = 'leongu_test',
      |'properties.security.protocol' = 'SASL_SDP',
      |'properties.sasl.mechanism' = 'SDP',
      |'properties.kafka.security.authentication.sdp.publickey' = 'yzMVgK2iWzctULWGvrYRSyFYTFVqd6p5ppoo',
      |'properties.kafka.security.authentication.sdp.privatekey' = 'SE1AbMLVg0QUq7cCvlDNvLtC421iKxij',
      |'scan.startup.mode' = 'earliest-offset',
      |'format' = 'json',
      |'json.ignore-parse-errors' = 'true',
      |'json.fail-on-missing-field' = 'false')
      |""".stripMargin

  def main(args: Array[String]) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val env = Utils.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    env.getCheckpointConfig.setCheckpointInterval(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    tableEnv.executeSql(KAFKA_SOURCE)
    tableEnv.executeSql(KAFKA_SINK)
    tableEnv.executeSql(PRINT_SINK_SQL)
    //    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
    //    val tableResult1 = tableEnv.executeSql("insert into print_table select text from topic1")
    //    val tableResult1 = tableEnv.executeSql("insert into hdfs_table select text as content, 'a' as dt, 'b' as h from topic1")
    val tableResult1 = tableEnv.executeSql("insert into kafka2 select msg from kafka1")
//    val tableResult2 = tableEnv.executeSql("insert into print_table select msg from kafka1")
  }
}
