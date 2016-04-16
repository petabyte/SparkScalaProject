/**
 * Created by George on 4/15/2016.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{RDD, HadoopRDD}

object ProcessFileNames {
  def readFiles(sc:SparkContext): RDD[(String, String)] = {
    // Create the text file
    val text = sc.hadoopFile("userSearchLogs", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], sc.defaultMinPartitions)
    val hadoopRdd = text.asInstanceOf[HadoopRDD[LongWritable, Text]]
    val fileAndLine = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) =>
      val file = inputSplit.asInstanceOf[FileSplit]
      iterator.map { tpl => (file.getPath.getName, tpl._2.toString) }
    }
    return fileAndLine
  }
}
