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
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env = Util.localEnv

    val topic = "kafka_service_check"
    val properties = sdpKafkaProp
    val kafkaStream = env
      .addSource(new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), properties))
    kafkaStream.print()

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
    properties.setProperty("bootstrap.servers", "sdp-10-88-100-155:6668")
    properties.setProperty("group.id", "leongu_test")
    properties.setProperty("security.protocol", "SASL_SDP")
    properties.setProperty("sasl.mechanism", "SDP")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("kafka.security.authentication.sdp.publickey", "yzMVgK2iWzctULWGvrYRSyFYTFVqd6p5ppoor")
    properties.setProperty("kafka.security.authentication.sdp.privatekey", "SE1AbMLVg0QUq7cCvlDNvLtC421iKxij")
    properties
  }
}
