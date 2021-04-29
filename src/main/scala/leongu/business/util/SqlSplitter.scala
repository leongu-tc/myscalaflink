package leongu.business.util

import java.util

class SqlSplitter {
  // it must be either 1 character or 2 character
  private val singleLineCommentPrefixList = new util.HashSet[String]
  singleLineCommentPrefixList.add("--")

  def splitSql(txt: String): util.ArrayList[String] = {
    val text = txt.trim
    val queries = new util.ArrayList[String]
    var query = new StringBuilder
    var character:Char = 0
    var multiLineComment = false
    var singleLineComment = false
    var singleQuoteString = false
    var doubleQuoteString = false
    var index = 0
    while ( {
      index < text.length
    }) {
      character = text.charAt(index)
      // end of single line comment
      if (singleLineComment && (character == '\n')) {
        singleLineComment = false
        if (query.toString.trim.isEmpty) {}
      }
      // end of multiple line comment
      if (multiLineComment && character == '/' && text.charAt(index - 1) == '*') {
        multiLineComment = false
        if (query.toString.trim.isEmpty) {}
      }
      if (character == '\'') if (singleQuoteString) singleQuoteString = false
      else if (!doubleQuoteString) singleQuoteString = true
      if (character == '"') if (doubleQuoteString && index > 0) doubleQuoteString = false
      else if (!singleQuoteString) doubleQuoteString = true
      if (!singleQuoteString && !doubleQuoteString && !multiLineComment && !singleLineComment && text.length > (index + 1)) if (isSingleLineComment(text.charAt(index), text.charAt(index + 1))) singleLineComment = true
      else if (text.charAt(index) == '/' && text.charAt(index + 1) == '*') multiLineComment = true
      if (character == ';' && !singleQuoteString && !doubleQuoteString && !multiLineComment && !singleLineComment) { // meet semicolon
        queries.add(query.toString.trim)
        query = new StringBuilder
      }
      else if (index == (text.length - 1)) { // meet the last character
        if (!singleLineComment && !multiLineComment) {
          query.append(character)
          queries.add(query.toString.trim)
        }
      }
      else if (!singleLineComment && !multiLineComment) { // normal case, not in single line comment and not in multiple line comment
        query.append(character)
      }
      else if (singleLineComment && !query.toString.trim.isEmpty) { // in single line comment, only add it to query when the single line comment is
        // in the middle of sql statement
        // e.g.
        // select a -- comment
        // from table_1
        query.append(character)
      }
      else if (multiLineComment && !query.toString.trim.isEmpty) { // in multiple line comment, only add it to query when the multiple line comment
        // is in the middle of sql statement.
        // select a /* comment */
        query.append(character)
      }

      {
        index += 1; index - 1
      }
    }
    queries
  }

  private def isSingleLineComment(curChar: Char, nextChar: Char): Boolean = {
    import scala.collection.JavaConversions._
    for (singleCommentPrefix <- singleLineCommentPrefixList) {
      if (singleCommentPrefix.length == 1) if (curChar == singleCommentPrefix.charAt(0)) return true
      if (singleCommentPrefix.length == 2) if (curChar == singleCommentPrefix.charAt(0) && nextChar == singleCommentPrefix.charAt(1)) return true
    }
    false
  }
}
