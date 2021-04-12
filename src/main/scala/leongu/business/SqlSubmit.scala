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
      env.getCheckpointConfig.setCheckpointInterval(conf.getOrElse[Long](Utils.CP_INTERVAL, 180000))
    }
    if (conf.contains(Utils.MAX_CURR_CP)) {
      env.getCheckpointConfig.setMaxConcurrentCheckpoints(conf.getOrElse[Integer](Utils.MAX_CURR_CP, 3))
    }
    if (conf.contains(Utils.MIN_PAUSE_CP)) {
      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(conf.getOrElse[Long](Utils.MIN_PAUSE_CP, 60000))
    }
    if (conf.contains(Utils.CP_MODE)) {
      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.valueOf(conf.getOrElse[String](Utils.CP_MODE, Utils.ALOS.name())));
    }
    //    val env = Util.localEnv
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.executeSql(sql)
  }
}
