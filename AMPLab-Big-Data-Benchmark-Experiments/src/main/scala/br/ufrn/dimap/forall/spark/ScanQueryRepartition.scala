package br.ufrn.dimap.forall.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.sql.Date
import org.apache.spark.sql.SparkSession

/*
 * 1. Scan Query
 * 
 * SELECT pageURL, pageRank FROM rankings WHERE pageRank > X
 * 
 * This query scans and filters the dataset and stores the results.
 */
object ScanQueryRepartition {

  /*
   * Rankings
   * 
   * Lists websites and their page rank	
   * 
   * pageURL VARCHAR(300)
   * pageRank INT
   * avgDuration INT
   */
  case class Ranking(pageURL: String, pageRank: Int, avgDuration: Int)

  def parseRankings(line: String): Ranking = {
    val fields = line.split(',')
    val ranking: Ranking = Ranking(fields(0), fields(1).toInt, fields(2).toInt)
    return ranking
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var inputRankingsURL = "hdfs://master:54310/user/hduser/BigBenchDataSet/rankings" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/page-ranks" // default value

    if (args.length > 1) {
      inputRankingsURL = args(0)
      outputURL = args(1)
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("AMPLab-Big-Data-Benchmark-1-Scan-Query-Repartition")
    val sparkContext = new SparkContext(conf)

    val rankingsLines = sparkContext.textFile(inputRankingsURL)
    val rankings = rankingsLines.map(parseRankings).repartition(50)

    val results = rankings.filter(r => r.pageRank > 300).map(r => (r.pageURL, r.pageRank))

    results.saveAsTextFile(outputURL)
  }
}