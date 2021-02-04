package leongu.myscalaflink.table

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2HBase {
  val PRINT_SINK_SQL =
    """
      |CREATE TABLE print_table (
      | text STRING
      | ) WITH ('connector' = 'print')
      |""".stripMargin
  val HBASE_SINK =
    """
      |CREATE TABLE hbase1 (
      | rowkey STRING,
      | cf ROW<c1 STRING>,
      | PRIMARY KEY (rowkey) NOT ENFORCED
      |) WITH ( 'connector' = 'hbase-1.4',
      | 'table-name' = 'mytable',
      | 'zookeeper.quorum' = 'sdp-10-88-100-154:2181,sdp-10-88-100-155:2181,sdp-10-88-100-156:2181'
      | 'zookeeper.znode.parent' = '/hbase_unsecure'
      | )
      |""".stripMargin
  val KAFKA_SOURCE =
    """
      |CREATE TABLE kafka1 (
      | msg VARCHAR
      | ) WITH (
      |'connector' = 'kafka-0.10',
      |'topic' = 'kafka_service_check',
      |'properties.bootstrap.servers' = 'sdp-10-88-100-154:6668',
      |'properties.group.id' = 'leongu_test',
      |'properties.security.protocol' = 'SASL_SDP',
      |'properties.sasl.mechanism' = 'SDP',
      |'properties.key.deserializer' = 'org.apache.kafka.common.serialization.LongDeserializer',
      |'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
      |'format' = 'csv',
      |'scan.startup.mode' = 'earliest-offset')
      |""".stripMargin
  val FS_SINK =
    """
      |CREATE TABLE hdfs_table (
      |   content STRING,
      |   dt STRING,
      |   h STRING
      |) PARTITIONED BY (dt, h) WITH (
      |  'connector' = 'filesystem',
      |  'path '= 'hdfs://localhost:9000/tmp/zyh_test',
      |  'format' = 'csv'
      |)
      |""".stripMargin

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env = Util.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.executeSql(PRINT_SINK_SQL)
    //    tableEnv.executeSql(HBASE_SINK)
    tableEnv.executeSql(KAFKA_SOURCE)
    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
//    val tableResult1 = tableEnv.executeSql("insert into print_table select text from topic1")
//    val tableResult1 = tableEnv.executeSql("insert into hdfs_table select text as content, 'a' as dt, 'b' as h from topic1")
    val tableResult1 = tableEnv.executeSql("insert into hbase1 select msg as rowkey, ROW(msg) as cf from kafka1")
  }

  def localKafkaProp: Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "leongu_test")
    properties.setProperty("properties.key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer")
    properties.setProperty("properties.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties
  }

  def sdpKafkaProp: Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "sdp-10-88-100-154:6668")
    properties.setProperty("group.id", "leongu_test")
    properties.setProperty("properties.security.protocol", "SASL_SDP")
    properties.setProperty("properties.sasl.mechanism", "SDP")
    properties.setProperty("properties.key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer")
    properties.setProperty("properties.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties
  }
}
