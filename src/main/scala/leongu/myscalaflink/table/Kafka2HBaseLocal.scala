package leongu.myscalaflink.table

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka2HBaseLocal {
  val HBASE_SINK =
    """
      |CREATE TABLE hbase1 (
      | rowkey STRING,
      | cf ROW<c1 STRING>,
      | PRIMARY KEY (rowkey) NOT ENFORCED
      |) WITH ( 'connector' = 'hbase-1.4',
      | 'table-name' = 'mytable',
      | 'zookeeper.quorum' = 'localhost:2182'
      | )
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
      |'properties.key.deserializer' = 'org.apache.kafka.common.serialization.LongDeserializer',
      |'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
      |'format' = 'csv',
      |'scan.startup.mode' = 'earliest-offset')
      |""".stripMargin

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env = Util.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.executeSql(HBASE_SINK)
    tableEnv.executeSql(KAFKA_SOURCE)
    //    tableEnv.executeSql(FS_SINK)

    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
    //    val tableResult1 = tableEnv.executeSql("insert into print_table select text from topic1")
    //    val tableResult1 = tableEnv.executeSql("insert into hdfs_table select text as content, 'a' as dt, 'b' as h from topic1")
    val tableResult1 = tableEnv.executeSql("insert into hbase1 select msg as rowkey, ROW(msg) as cf from kafka1")
  }
}
