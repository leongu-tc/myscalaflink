package leongu.myscalaflink.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api._
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.api.scala._
import java.util.Properties

import leongu.myscalaflink.util.HBaseUtils
import org.apache.flink.connector.hbase.sink.HBaseDynamicTableSink

object Kafka2HBase {
  val PRINT_SINK_SQL =
    "CREATE TABLE print_table " +
      "( \n" + " text STRING \n) " +
      "with (\n'connector' = 'print'\n)"

  val HBASE_SINK = "CREATE TABLE ht1 " +
    "(\n rowkey STRING,\n " +
    "cf ROW<c1 STRING>,\n " +
    "PRIMARY KEY (rowkey) NOT ENFORCED\n) " +
    "WITH (\n 'connector' = 'hbase-1.4',\n " +
    "'table-name' = 'mytable',\n " +
    "'zookeeper.quorum' = 'localhost:2182'\n)"

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env = Util.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.executeSql(PRINT_SINK_SQL)
    //    tableEnv.executeSql(HBASE_SINK)

    val topic = "topic1"
    //    val topic = "kafka_service_check"
    val properties = localKafkaProp
    val kafkaStream = env
      .addSource(new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), properties))

    //      val kafkaTable: Table = tableEnv.fromDataStream(kafkaStream, $"text", $"rowtime")
    val kafkaTable: Table = tableEnv.fromDataStream(kafkaStream, $"text")
    kafkaTable.printSchema()
    //  TODO ERROR  kafkaTable.select($"text".as("rowkey"), $"ROW(text)")
    //        .executeInsert("print_table")
    //    val tableResult1 = tableEnv.executeSql("insert into ht1 select text as rowkey, ROW(text) as cf from topic1")
    val tableResult1 = tableEnv.executeSql("insert into print_table select text from topic1")

    val schema =
      TableSchema.builder()
        .field("rowkey", STRING())
        .field("cf", ROW(FIELD("c1", STRING()), FIELD("rowtime", STRING())))
        .build()
    val hbaseOptions = new java.util.HashMap[String, String]
    hbaseOptions.put("connector", "hbase-1.4")
    hbaseOptions.put("table-name", "hbase1")
    hbaseOptions.put("zookeeper.quorum", "sdp-10-88-100-154:2181,sdp-10-88-100-155:2181,sdp-10-88-100-156:2181")
    hbaseOptions.put("zookeeper.znode.parent", "/hbase-unsecure")
    val sink = HBaseUtils.createTableSink(schema, hbaseOptions)
    val hbaseSink = sink.asInstanceOf[HBaseDynamicTableSink]

    println("-----------1 ")
    env.execute("Kafka2HBase")
    println("-----------2 ")
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
