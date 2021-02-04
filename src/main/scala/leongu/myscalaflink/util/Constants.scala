package leongu.myscalaflink.util

import java.io.File

object Constants {
  var projPath: String = new File(Thread.currentThread.getContextClassLoader.getResource("").getPath)
    .getParentFile.getParentFile.getPath
  var res: String = projPath + "/src/main/resources/"
  var target: String = projPath + "/target/"
}
