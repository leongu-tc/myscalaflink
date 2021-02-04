package leongu.myscalaflink.util


import java.io.File

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Utils {
  /**
   * local run with web ui localhost:18081
   *
   * @return
   */
  def localEnv: StreamExecutionEnvironment = {
    val flink_conf = new Configuration
    flink_conf.setLong("rest.port", 18081)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flink_conf)
    env
  }

  /**
   * mac standalone cluster localhost:8081 , with myflink.jar
   *
   * @return
   */
  def remoteEnv: StreamExecutionEnvironment = {
    val jarPath = Constants.target + "myflink-1.0.0.jar"
    System.out.println(jarPath)
    val env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081, jarPath)
    env
  }

  def deleteDir(dir: File): Unit = {
    if (!dir.exists) return
    val files = dir.listFiles
    for (f <- files) {
      if (f.isDirectory) deleteDir(f)
      else {
        f.delete
        System.out.println("delete file " + f.getAbsolutePath)
      }
    }
    dir.delete
    System.out.println("delete dir " + dir.getAbsolutePath)
  }

  def deleteDir(dir: String): Unit = {
    deleteDir(new File(dir))
  }
}
