package leongu.business

import leongu.business.util.Utils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.collection.mutable

object SqlSubmit {
  var conf: mutable.Map[String, Object] = mutable.Map()

  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)
    var sql = parameterTool.get("sql")
    val confFile = parameterTool.get("conf")
    val sqlFile = parameterTool.get("sqlfile")
    if (StringUtils.isEmpty(sql) && StringUtils.isEmpty(sqlFile)) {
      throw new Exception("args: --sql and --sqlfile cannot be both empty!")
    } else if (!StringUtils.isEmpty(sql) && !StringUtils.isEmpty(sqlFile)) {
      throw new Exception("args: --sql and --sqlfile cannot be both valued!")
    } else if (!StringUtils.isEmpty(sqlFile)) {
      sql = Utils.readSql(sqlFile)
    }

    if (!StringUtils.isEmpty(confFile)) {
      conf = Utils.readConfFromResources(confFile)
      if (StringUtils.isBlank(sql)) {
        sql = conf.getOrElse(Utils.QUERY, "").toString
      }
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    if (conf.contains(Utils.CP_INTERVAL)) {
      env.getCheckpointConfig.setCheckpointInterval(conf.getOrElse(Utils.CP_INTERVAL, 180000).toString.toLong)
    }
    if (conf.contains(Utils.MAX_CURR_CP)) {
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(conf.getOrElse(Utils.MAX_CURR_CP, 3).toString.toInt)
    }
    if (conf.contains(Utils.MIN_PAUSE_CP)) {
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(conf.getOrElse(Utils.MIN_PAUSE_CP, 60000).toString.toLong)
    }
    if (conf.contains(Utils.CP_MODE)) {
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.valueOf(conf.getOrElse(Utils.CP_MODE, Utils.ALOS.name()).toString));
    }
    //    val env = Util.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.executeSql(sql)
  }
}
