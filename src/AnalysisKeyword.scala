import java.math.RoundingMode
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Calendar
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by George on 4/15/2016.
 */
object AnalysisKeyword {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Process Keyword Logs")
    val sc = new SparkContext(sparkConf)
    val keyWordMapping = ProcessFileNames.readFiles(sc)
      .map(x => createMapping(x)).cache()
    val totalNumberOfKeywords = keyWordMapping.count()
    val countKeywords = keyWordMapping.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).cache()
    val topTaxSeasonKeywords = countKeywords.map(x => (x._2._1, x._1)).sortByKey(false).top(10)
    val top10TaxSeasonKeywords = sc.parallelize(topTaxSeasonKeywords, 1)
    top10TaxSeasonKeywords.saveAsTextFile("top10TaxSeasonKeywords")
    val topKeywords = countKeywords.map(x => (x._2._2, x._1)).sortByKey(false).top(10)
    val top10Keywords = sc.parallelize(topKeywords, 1)
    top10Keywords.saveAsTextFile("top10Keywords")
    val keyWordsAnalysis = countKeywords.map(x => comparativeAnalysis(x, totalNumberOfKeywords)).sortByKey(false).top(10)
    val top10KeywordAnalysis = sc.parallelize(keyWordsAnalysis, 1)
    top10KeywordAnalysis.saveAsTextFile("top10AnalysisKeywords")
    System.exit(0)

  }

  def createMapping(lineRecord: (String, String)): (String, (Int, Int)) = {
    try {
      val df = new SimpleDateFormat("yyyyMMdd")
      val calendar = Calendar.getInstance()
      val fileDateString = lineRecord._1.split("_")(2).split("\\.")(0)
      calendar.setTime(df.parse(fileDateString))
      val month = calendar.get(Calendar.MONTH)
      var taxSeason = 0
      if (month == Calendar.MARCH || month == Calendar.APRIL) {
        taxSeason = 1
      }
      val keyword = KeywordParser.parseKeyWord(lineRecord._2)
      return (keyword, (taxSeason, 1))
    } catch {
      case aioe: ArrayIndexOutOfBoundsException => return null
      case e: Exception => null
    }
  }

  def comparativeAnalysis(mappedRecords: (String, (Int, Int)), totalNumberOfKeywords: Long): (Double, String) = {
   try{
    val df: DecimalFormat = new DecimalFormat("#.#####")
        df.setRoundingMode(RoundingMode.CEILING)
    val totalKeywordForTaxSeason = mappedRecords._2._1.toDouble
    val totalKeyword = mappedRecords._2._2.toDouble
    val chiTaxSeason: Double = Math.pow(totalKeywordForTaxSeason - totalKeyword, 2.0) / totalKeyword
    val chiAllPopulation: Double = Math.pow(totalKeyword - totalNumberOfKeywords, 2.0) / totalNumberOfKeywords
    val expected: Double = chiTaxSeason / chiAllPopulation
    val chiValue: Double = if (Double.NaN != expected) expected else 0.0
    val chiValueFormatted: Double = df.format(chiValue).toDouble
    return (chiValueFormatted, mappedRecords._1)
   }catch{
     case e: Exception => return null
   }
  }

}
