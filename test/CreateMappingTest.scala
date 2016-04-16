import org.scalatest.FlatSpec

/**
 * Created by George on 4/16/2016.
 */
class CreateMappingTest extends FlatSpec{
  "A CreateMapping" should "have created a tuple3" in {
    assert(AnalysisKeyword.createMapping(("CP_UserSearch_20160306.zip.queries.log","10_01BCEA5099D956DCE55F349110EEBF72\t1testKeyword1\t1\t2\tTC\t60\tv1\t366")) === ("1testkeyword1",(1,1)))
    assert(AnalysisKeyword.createMapping(("CP_UserSearch_20160406.zip.queries.log","10_01BCEA5099D956DCE55F349110EEBF72\t1testKeyword1\t1\t2\tTC\t60\tv1\t366")) === ("1testkeyword1",(1,1)))
    assert(AnalysisKeyword.createMapping(("CP_UserSearch_20160506.zip.queries.log","10_01BCEA5099D956DCE55F349110EEBF72\t1testKeyword1\t1\t2\tTC\t60\tv1\t366")) === ("1testkeyword1",(0,1)))
    assert(AnalysisKeyword.createMapping(("CP_UserSearch_20160606.zip.queries.log","badData")) === (null,(0,1)))
    assert(AnalysisKeyword.createMapping(("badData","badData")) === null)
  }

  "A ComparativeAnalysis" should "have created a tuple2" in {
    assert(AnalysisKeyword.comparativeAnalysis(("testkeyword0",(0,4)),15) === (0.49587,"testkeyword0"))
    assert(AnalysisKeyword.comparativeAnalysis(("testkeyword1",(2,3)),15) === (0.03473,"testkeyword1"))
    assert(AnalysisKeyword.comparativeAnalysis(("testkeyword2",(3,5)),15) === (0.12,"testkeyword2"))
    assert(AnalysisKeyword.comparativeAnalysis(("testkeyword3",(4,6)),15) === (0.12346,"testkeyword3"))
    assert(AnalysisKeyword.comparativeAnalysis(("testkeyword4",null),10) === null)
  }
}
