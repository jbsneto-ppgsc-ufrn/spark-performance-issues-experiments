package br.ufrn.dimap.forall.spark

import java.sql.Date

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * 3. Join Query
 * 
 * SELECT sourceIP, totalRevenue, avgPageRank
 * 		FROM
 * 			(SELECT sourceIP,
 * 							AVG(pageRank) as avgPageRank,
 * 							SUM(adRevenue) as totalRevenue
 * 			FROM Rankings AS R, UserVisits AS UV
 * 			WHERE R.pageURL = UV.destURL
 * 				AND UV.visitDate BETWEEN Date(`1980-01-01') AND Date(`X')
 * 			GROUP BY UV.sourceIP)
 * 		ORDER BY totalRevenue DESC LIMIT 1
 * 
 * This query joins a smaller table to a larger table then sorts the results.
 */
object JoinQuerySamePartitioners {

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
    
    var inputRankingsURL = "hdfs://master:54310/user/hduser/BigBenchDataSet/rankings" // default value
    var inputUserVisitsURL = "hdfs://master:54310/user/hduser/BigBenchDataSet/uservisits" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/join-query-results" // default value

    if (args.length > 2) {
      inputRankingsURL = args(0)
      inputUserVisitsURL = args(1)
      outputURL = args(2)
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("AMPLab-Big-Data-Benchmark-3-Join-Query-Same-Partitioners")
    val sparkContext = new SparkContext(conf)

    val rankingsLines = sparkContext.textFile(inputRankingsURL)
    val rankings = rankingsLines.map(parseRankings)

    val userVisitsLines = sparkContext.textFile(inputUserVisitsURL)
    val userVisits = userVisitsLines.map(parseUserVisits)

    val date1 = Date.valueOf("1980-01-01")
    val date2 = Date.valueOf("1980-04-01")
    val subqueryUV = userVisits.filter(u => u.visitDate.after(date1) && u.visitDate.before(date2)).map(u => (u.destURL, u)).partitionBy(new StringPartitioner(42))
    val subqueryR = rankings.map(r => (r.pageURL, r)).partitionBy(new StringPartitioner(42))
    val subqueryJoin = subqueryR.join(subqueryUV)
    val subquerySelect = subqueryJoin.values.map(v => (v._2.sourceIP, (v._2.adRevenue, v._1.pageRank)))
    val subqueryAggregation = subquerySelect.groupByKey().map(v => (v._1, v._2.map(_._1).sum, v._2.map(_._2).sum / v._2.map(_._2).size))

    val results = subqueryAggregation.sortBy(_._2, false)

    results.saveAsTextFile(outputURL)
  }

  class StringPartitioner(np: Int) extends Partitioner {

    override def numPartitions: Int = np

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[String]
      var p = k.hashCode() % numPartitions
      if (p < 0)
        p + numPartitions // make it non-negative
      else
        p
    }

    override def equals(other: Any): Boolean = {
      other match {
        case obj: StringPartitioner => obj.numPartitions == numPartitions
        case _                      => false
      }
    }
  }
}