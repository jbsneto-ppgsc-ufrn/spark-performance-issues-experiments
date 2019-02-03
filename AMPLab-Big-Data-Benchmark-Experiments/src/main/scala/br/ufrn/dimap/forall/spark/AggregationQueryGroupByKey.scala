package br.ufrn.dimap.forall.spark

import java.sql.Date
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * 2. Aggregation Query
 * 
 * SELECT SUBSTR(sourceIP, 1, X), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, X)
 * 
 * This query applies string parsing to each input tuple then performs a high-cardinality aggregation.
 */
object AggregationQueryGroupByKey {

  /*
   * UserVisits
   * 
   * Stores server logs for each web page
   * 
   * sourceIP VARCHAR(116)
   * destURL VARCHAR(100)
   * visitDate DATE
   * adRevenue FLOAT
   * userAgent VARCHAR(256)
   * countryCode CHAR(3)
   * languageCode CHAR(6)
   * searchWord VARCHAR(32)
   * duration INT
   */
  case class UserVisit(sourceIP: String, destURL: String, visitDate: Date,
                       adRevenue: Float, userAgent: String, countryCode: String,
                       languageCode: String, searchWord: String, duration: Int)

  def parseUserVisits(line: String): UserVisit = {
    val fields = line.split(',')
    val userVisit: UserVisit = UserVisit(fields(0), fields(1), Date.valueOf(fields(2)),
      fields(3).toFloat, fields(4), fields(5),
      fields(6), fields(7), fields(8).toInt)
    return userVisit
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var inputUserVisitsURL = "hdfs://master:54310/user/hduser/BigBenchDataSet/uservisits" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/aggregation-query" // default value

    if (args.length > 1) {
      inputUserVisitsURL = args(0)
      outputURL = args(1)
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("AMPLab-Big-Data-Benchmark-2-Aggregation-Query-GroupByKey")
    val sparkContext = new SparkContext(conf)

    val userVisitsLines = sparkContext.textFile(inputUserVisitsURL)
    val userVisits = userVisitsLines.map(parseUserVisits)

    val results = userVisits.map(u => (u.sourceIP.substring(0, 7), u.adRevenue)).groupByKey().map(v => (v._1, v._2.sum))

    results.saveAsTextFile(outputURL)
  }
}