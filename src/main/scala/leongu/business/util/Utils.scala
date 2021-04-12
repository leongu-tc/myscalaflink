package leongu.business.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Objects}

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object Utils extends Cons {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  def readSql(file: String): String = {
    var sql: String = null
    if (file != null) {
      val source = Source.fromFile(file)
      sql = source.mkString
      source.close()
    }

    LOG.info("------------ Config -----------")
    LOG.info(sql)
    LOG.info("------------ End --------------")
    sql
  }

  def readConf(file: String): mutable.Map[String, Object] = {
    var conf: mutable.Map[String, Object] = mutable.Map()
    if (file != null) {
      val stream = Source.fromFile(file).reader()
      val yaml = new Yaml()
      val javaConf = yaml.load(stream).asInstanceOf[java.util.HashMap[String, Object]]
      stream.close()
      conf = JavaConverters.mapAsScalaMapConverter(javaConf).asScala
    }

    LOG.info("------------ Config -----------")
    conf.map(kv => LOG.info(kv.toString()))
    LOG.info("------------ End --------------")
    conf
  }


  def readConfFromResources(file: String): mutable.Map[String, Object] = {
    var conf: mutable.Map[String, Object] = mutable.Map()
    if (file != null) {
      val stream = Source.fromURL(getClass.getResource(file)).bufferedReader()
      if (Objects.isNull(stream)) {
        throw new Exception("can not read " + file)
      }
      val yaml = new Yaml()
      val javaConf = yaml.load(stream).asInstanceOf[java.util.HashMap[String, Object]]
      stream.close()
      conf = JavaConverters.mapAsScalaMapConverter(javaConf).asScala
    }
    LOG.info("------------ Config -----------")
    conf.map(kv => LOG.info(kv.toString()))
    LOG.info("------------ End --------------")
    conf
  }

  def yesterday(): Calendar = {
    val day = Calendar.getInstance()
    day.add(Calendar.DATE, -1)
    day
  }

  def yesterdayStr(df: SimpleDateFormat): String = {
    df.format(yesterday.getTime)
  }

  /**
   * azkaban's SDP_DATA_TIME (yyyyMMdd) has top priority
   * jobDate -1: full history
   * not set: use yesterday
   *
   * @param conf
   * @param jobDateConf
   * @param df jobDate format
   * @return also jobDate format
   */
  def jobDateFn(conf: mutable.Map[String, Object], jobDateConf: String, df: SimpleDateFormat): String = {
    if (conf.contains(jobDateConf)) {
      conf.getOrElse(jobDateConf, "-1").toString // use the config date
    } else {
      // 1 use SDP_DATE_TIME, 2 use yesterday
      var sdpDataTimeStr = System.getenv("SDP_DATA_TIME")
      LOG.info(s"SDP_DATA_TIME $sdpDataTimeStr")
      if (sdpDataTimeStr != null && sdpDataTimeStr.length > 0) {
        df.format(DF1.parse(sdpDataTimeStr)).toString
      } else {
        yesterdayStr(df)
      }
    }
  }

}
