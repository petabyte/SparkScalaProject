
/**
 * Created by George on 4/14/2016.
 */
object KeywordParser {
  def parseKeyWord(lineRecordText: String): String = {
    try {
      val keyword = lineRecordText.split("\\t+")(1)
      if (keyword != null && keyword.length > 0) {
        return keyword.replaceAll("\\s+"," ")
        //Replace double - double quotes
        .replaceAll("^\"\"", "\"")
       //Replace double -  double quotes at the end
        .replaceAll("\"\"$","\"")
        //Remove space after quote if keyword has quote
        .replaceAll("^\"\\s", "\"")
        //Remove space before closing quote
        .replaceAll("\\s\"$","\"")
          .toLowerCase()
      }
      return null
    } catch {
      case aioe: ArrayIndexOutOfBoundsException => return null
      case e: Exception => return null
    }
  }
}
