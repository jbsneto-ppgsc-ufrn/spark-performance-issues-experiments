package br.ufrn.dimap.forall.spark

import scala.math.sqrt

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MoviesRecommendation {

  // ratings.csv has the following data: userId,movieId,rating,timestamp
  // we want (userId,(movieId, rating))
  def parseRatings(r: String) = {
    try {
      val data = r.split("::")
      val userId = data(0).toInt
      val movieId = data(1).toInt
      val rating = data(2).toDouble
      Some((userId, (movieId, rating)))
    } catch {
      case _: Throwable => None
    }
  }

  // filter duplicates by keeping only the pairs which the first movie id is less than the second movie id 
  def removeDuplicates(u: (Int, ((Int, Double), (Int, Double)))): Boolean = {
    val movieId1 = u._2._1._1
    val movieId2 = u._2._2._1
    return movieId1 < movieId2
  }

  // we get the pair of correlated movies and the pair of their ratings
  def makeCorrelatedMoviesPairs(u: (Int, ((Int, Double), (Int, Double)))) = {
    val movieId1 = u._2._1._1
    val rating1 = u._2._1._2
    val movieId2 = u._2._2._1
    val rating2 = u._2._2._2
    ((movieId1, movieId2), (rating1, rating2))
  }

  // calculate the similarity between two movies using the cosine-base similarity
  // returns (similarity, number of pairs)
  def similarity(pairs: Iterable[(Double, Double)]): (Double, Int) = {
    var sumAA: Double = 0.0
    var sumBB: Double = 0.0
    var sumAB: Double = 0.0

    for (pair <- pairs) {
      val A = pair._1
      val B = pair._2
      sumAA += A * A
      sumBB += B * B
      sumAB += A * B
    }

    val num: Double = sumAB
    val den = sqrt(sumAA) * sqrt(sumBB)

    var sim: Double = 0.0
    if (den != 0) {
      sim = num / den
    }
    (sim, pairs.size)
  }

  // filter similarities greater than or equal to 0.9 and number of pairs greater than or equal to 900
  // we use 0.9 and 900 as thresholds to get "relevant" similarities to a better recommendation, this values can be changed according to different criteria
  def relevantSimilarities(sim: ((Int, Int), (Double, Int))) = {
    sim._2._1 > 0.9 && sim._2._2 > 900
  }

  // we make a tuple of (movie id, (similar movie id, similarity score))
  // to get the top 10 recommended movies for each movie we do a redundancy to group by movie id
  def makeMoviesSimilaritiesPairs(sim: ((Int, Int), (Double, Int))) = {
    val movieId = sim._1._1
    val similarMovieId = sim._1._2
    val similarityScore = sim._2._1
    Array((movieId, (similarMovieId, similarityScore)), (similarMovieId, (movieId, similarityScore)))
  }

  // we create a sequence with movie id and the top 10 recommended movies based on this movie in csv format
  // movieId,recommendedMovieId1,recommendedMovieId2,recommendedMovieId3,...,recommendedMovieId10
  def makeTop10RecommendedMoviesCSV(similarMovies: (Int, Iterable[(Int, Double)])) = {
    val movieId = similarMovies._1
    val sortedSimilarMovies = similarMovies._2.toList.sortBy(_._2).reverse
    var recommendedMovies = movieId.toString
    var i = 0
    while (i < 10 && i < sortedSimilarMovies.size) {
      recommendedMovies += "," + sortedSimilarMovies(i)._1.toString
      i += 1
    }
    recommendedMovies
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    var inputML1MRatingsURL = "hdfs://master:54310/user/hduser/ml-1m/ratings.dat" // default value
    var outputSimilaritiesURL = "hdfs://master:54310/user/hduser/Output/movies-similarities" // default value
    var outputRecommendationURL = "hdfs://master:54310/user/hduser/Output/movies-recommendation" // default value
    var n: Int = 24 // default value

    if (args.length > 3 && args(3).toInt > 1) {
      inputML1MRatingsURL = args(0)
      outputSimilaritiesURL = args(1)
      outputRecommendationURL = args(2)
      n = args(3).toInt
    } else {
      println("Invalid arguments")
    }

    val numberPartitions: Int = n

    val conf = new SparkConf()
    conf.setAppName("MovieLens-Movies-Recommendation-with-" + numberPartitions.toString + "-Partitions")
    val sparkContext = new SparkContext(conf)

    val ratings = sparkContext.textFile(inputML1MRatingsURL).flatMap(parseRatings).partitionBy(new HashPartitioner(numberPartitions))

    val selfJoinRatings = ratings.join(ratings)

    val filteredSelfJoinRatings = selfJoinRatings.filter(removeDuplicates)

    val correlatedMovies = filteredSelfJoinRatings.map(makeCorrelatedMoviesPairs).partitionBy(new HashPartitioner(numberPartitions))

    val correlatedMoviesGroupedRatings = correlatedMovies.groupByKey()

    val moviesSimilarities = correlatedMoviesGroupedRatings.mapValues(similarity)

    val sortedMoviesSimilarities = moviesSimilarities.sortByKey()

    sortedMoviesSimilarities.saveAsTextFile(outputSimilaritiesURL)

    val relevantMoviesSimilarities = sortedMoviesSimilarities.filter(relevantSimilarities)

    val recommendedMoviesPairs = relevantMoviesSimilarities.flatMap(makeMoviesSimilaritiesPairs)

    val recommendedMoviesList = recommendedMoviesPairs.groupByKey()

    val sortedRecommendedMoviesList = recommendedMoviesList.sortByKey()

    val top10RecommendedMoviesByMovie = sortedRecommendedMoviesList.map(makeTop10RecommendedMoviesCSV)

    top10RecommendedMoviesByMovie.saveAsTextFile(outputRecommendationURL)
  }
}