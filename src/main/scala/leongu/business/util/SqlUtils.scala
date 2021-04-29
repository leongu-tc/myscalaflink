package leongu.business.util

import java.util
import java.util.regex.Pattern

object SqlUtils {
  val pSqlComment: Pattern = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|/\\*[^+].*?\\*/|#.*?$|", // won't remove Hints
    //      .compile("(?ms)('(?:''|[^'])*')|--.*?$|/\\*.*?\\*/|#.*?$|", // will remove Hints /*+ ...*/
    Pattern.CASE_INSENSITIVE)

  def readSqlsFromText(text: String): util.ArrayList[String] = {
    val presult = pSqlComment.matcher(text).replaceAll("$1")
    val splitter = new SqlSplitter
    splitter.splitSql(presult)
  }

  def main(args: Array[String]): Unit = {
    val script = Utils.readSql("/Users/apple/Downloads/rtastdi_marg_cptl.sql")
    printf(script)
    import scala.collection.JavaConverters._
    SqlUtils.readSqlsFromText(script).asScala.foreach(sql => {
      printf("===============")
      printf(sql)
    })
  }
}
