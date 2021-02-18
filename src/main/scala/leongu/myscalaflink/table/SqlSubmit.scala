package leongu.myscalaflink.table

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object SqlSubmit {
  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)
    val sql = parameterTool.get("sql")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env = Util.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.executeSql(sql)
  }
}
