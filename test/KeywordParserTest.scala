import org.scalatest.FlatSpec

/**
 * Created by George on 4/15/2016.
 */
class KeywordParserTest extends FlatSpec {
  "A KeywordParser" should "have parsed 1testkeyword1" in {
    assert(KeywordParser.parseKeyWord("10_01BCEA5099D956DCE55F349110EEBF72\t1testKeyword1\t1\t2\tTC\t60\tv1\t366") === "1testkeyword1")
    assert(KeywordParser.parseKeyWord("10_01BCEA5099D956DCE55F349110EEBF72\t\"     1testKeyword1    \"\t1\t2\tTC\t60\tv1\t366") === "\"1testkeyword1\"")
    assert(KeywordParser.parseKeyWord("10_01BCEA5099D956DCE55F349110EEBF72\t\"\"   giftTax   \"\"\t1\t2\tTC\t60\tv1\t366") === "\"gifttax\"")
    assert(KeywordParser.parseKeyWord("10_01BCEA5099D956DCE55F349110EEBF72\tdog and cat\t1\t2\tTC\t60\tv1\t366") === "dog and cat")
    assert(KeywordParser.parseKeyWord("badData") === null)
  }
}
