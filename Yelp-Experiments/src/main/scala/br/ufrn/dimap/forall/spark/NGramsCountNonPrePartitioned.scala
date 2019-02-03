package br.ufrn.dimap.forall.spark

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object NGramsCountNonPrePartitioned {

  // Start and end marks of a sentence
  val start = "<start>"
  val end = "<end>"

  /*
   * Calculate the n-grams of a given sentence.
   */
  def nGrams(n: Int, sentence: String): List[List[String]] = {
    val sentenceLowerCase = sentence.trim.toLowerCase
    val tokens = sentenceLowerCase.split(' ').map(t => t.replaceAll("""\W""", "")).filter(_.length() > 0).toList
    val tokensStartEnd = List.fill(n - 1)(start) ++ tokens :+ end
    val ngrams = tokensStartEnd.sliding(n)
    ngrams.toList
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    var inputURL = "hdfs://master:54310/user/hduser/Yelp/reviews" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/ngram-count" // default value
    var n: Int = 3 // default value

    if (args.length > 2 && args(2).toInt > 1) {
      inputURL = args(0)
      outputURL = args(1)
      n = args(2).toInt
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("NGrams-Count-reduceByKey-non-pre-partitioned-Version-with-" + n.toString)
    val sparkContext = new SparkContext(conf)

    val input = sparkContext.textFile(inputURL)

    val sentences = input.flatMap(x => x.split("(?<=[a-z])\\.\\s+"))

    val ngrams = sentences.flatMap(nGrams(n, _))

    val ngramsFiltered = ngrams.filter(l => l.filter(w => !w.trim.isEmpty).size == n && l != List.fill(n - 1)(start) :+ end) // removing empty n-grams

    val ngramsCount = ngramsFiltered.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    ngramsCount.saveAsTextFile(outputURL)
  }
}