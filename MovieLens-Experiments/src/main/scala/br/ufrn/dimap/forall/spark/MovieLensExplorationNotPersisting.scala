package br.ufrn.dimap.forall.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MovieLensExplorationNotPersisting {

  case class Movie(movieId: Int, title: String, year: Int, genres: List[String])
  case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)
  case class Tag(userId: Int, movieId: Int, tag: String, timestamp: Long)

  // All genres available
  val genresSet = Set("Action", "Adventure", "Animation", "Children's", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western", "(no genres listed)")

  // this functions extracts the year of the movie that is attached with the movie title. For example: "Toy Story (1995)"
  // -1 represents an unspecified year
  def extractMovieYear(movieTitle: String) = {
    val regex = "\\(([0-9]+)\\)".r
    val listNumbersTitle = regex.findAllIn(movieTitle).toList
    if (listNumbersTitle.nonEmpty)
      listNumbersTitle.last.replace("(", "").replace(")", "").trim.toInt
    else
      -1
  }

  // ratings.csv has the following data: userId,movieId,rating,timestamp
  // we want (userId, movieId, rating, timestamp)
  def parseRatings(r: String) = {
    try {
      val data = r.split(",")
      val userId = data(0).toInt
      val movieId = data(1).toInt
      val rating = data(2).toDouble
      val timestamp = data(3).toLong
      Some(Rating(userId, movieId, rating, timestamp))
    } catch {
      case _: Throwable => None
    }
  }

  // movies.csv has the following data: movieId,title,genres
  // we want (movieId,title, year, genres)
  def parseMovies(m: String) = {
    try {
      val data = m.split(",")
      val movieId = data(0).toInt
      val title = data(1)
      val year = extractMovieYear(title)
      val genres = data(2).split('|').toList.map(_.trim()).filter(p => genresSet.contains(p)) // removing unexpected genres
      Some(Movie(movieId, title, year, genres))
    } catch {
      case _: Throwable => None
    }
  }

  // R = average for the movie (mean) = (Rating)
  // v = number of votes for the movie = (votes)
  // m = minimum votes required to be listed in the Top 250
  // C = the mean vote across the whole report
  def weighted_rating(R: Double, v: Double, m: Double, C: Double) = {
    (v / (v + m)) * R + (m / (v + m)) * C
  }

  // tags.csv has the following data: userId,movieId,tag,timestamp
  // we want (userId,movieId,tag,timestamp)
  def parseTags(m: String) = {
    try {
      val data = m.split(",")
      val userId = data(0).toInt
      val movieId = data(1).toInt
      val tag = data(2)
      val timestamp = data(3).toLong
      Some(Tag(userId, movieId, tag, timestamp))
    } catch {
      case _: Throwable => None
    }
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    var inputMLLatestRatingsURL = "hdfs://master:54310/user/hduser/ml-latest/ratings.csv" // default value
    var inputMLLatestMoviesURL = "hdfs://master:54310/user/hduser/ml-latest/movies.csv" // default value
    var inputMLLatestTagsURL = "hdfs://master:54310/user/hduser/ml-latest/tags.csv" // default value
    var outputURL = "hdfs://master:54310/user/hduser/Output/"

    if (args.length > 3) {
      inputMLLatestRatingsURL = args(0)
      inputMLLatestMoviesURL = args(1)
      inputMLLatestTagsURL = args(2)
      outputURL = args(3)
    } else {
      println("Invalid arguments")
    }

    val conf = new SparkConf()
    conf.setAppName("MovieLens-Dataset-Exploration-Not-Persisting")
    val sparkContext = new SparkContext(conf)

    val ratings = sparkContext.textFile(inputMLLatestRatingsURL).flatMap(parseRatings)

    val movies = sparkContext.textFile(inputMLLatestMoviesURL).flatMap(parseMovies)

    val tags = sparkContext.textFile(inputMLLatestTagsURL).flatMap(parseTags)

    println("How many movies were produced per year?")

    val moviesPerYear = movies.map(m => (m.year, 1)).reduceByKey(_ + _).map(m => (m._2, m._1)).sortByKey(false)

    moviesPerYear.saveAsTextFile(outputURL + "movies-per-year")

    println("\nTop 10 of years with more movies:")
    moviesPerYear.take(10).foreach(m => println(m._2 + ": " + m._1 + " movies"))

    println("What were the most popular movie genres year by year?")

    val popularGenresByYear = movies.flatMap(m => m.genres.map(g => ((m.year, g), 1))).reduceByKey(_ + _).map(g => (g._1._1, (g._1._2, g._2))).reduceByKey((m1, m2) => if (m1._2 > m2._2) m1 else m2)

    popularGenresByYear.saveAsTextFile(outputURL + "popular-genres-by-year")

    println("\nMost popular genres by year:")
    popularGenresByYear.take(10).foreach(g => println("The most popular genre of " + g._1 + " was " + g._2._1 + " with " + g._2._2 + " movies"))

    println("What were the best movies of every decade (based on users’ ratings)?")

    val moviesIdKey = movies.map(m => (m.movieId, m))
    val ratingsMovieIdKey = ratings.map(r => (r.movieId, r))
    val joinMoviesRatings = moviesIdKey.join(ratingsMovieIdKey)
    val groupByMoviesInfo = joinMoviesRatings.map(m => ((m._1, m._2._1.title, m._2._1.year), m._2._2.rating))
    val ratingsStatistics = groupByMoviesInfo.groupByKey().mapValues(x => (x.size, x.sum / x.size, x.min, x.max))

    val sumMeanOfRatingsAllMovies = ratingsStatistics.map(m => (m._2._2, 1)).reduce((m1, m2) => (m1._1 + m2._1, m1._2 + m2._2))
    val meanMeanOfRatingsAllMovies = sumMeanOfRatingsAllMovies._1 / sumMeanOfRatingsAllMovies._2

    val ratingsStatisticsWithWR = ratingsStatistics.mapValues(x => (x._1, x._2, x._3, x._4, weighted_rating(x._2, x._1.toDouble, 500.0, meanMeanOfRatingsAllMovies)))
    val groupByDecade = ratingsStatisticsWithWR.map(m => (m._1._3 / 10 * 10, (m._1._2, m._2))).groupByKey()
    val bestMoviesDecade = groupByDecade.mapValues(s => s.reduce((s1, s2) => if (s1._2._5 > s2._2._5) s1 else s2)).sortByKey(true)

    bestMoviesDecade.saveAsTextFile(outputURL + "best-movies-decade")

    println("\nBest movies of every decade:")
    bestMoviesDecade.collect().foreach(m => println("Best movie from decade " + m._1 + " was " + m._2._1 + " with weighted rating of " + m._2._2._5))

    println("\nWhat tags best summarize a movie genre?")

    val tagsMovieIdKey = tags.map(t => (t.movieId, t.tag))
    val joinMoviesTags = moviesIdKey.join(tagsMovieIdKey)
    val genresTags = joinMoviesTags.flatMap(mg => mg._2._1.genres.map(g => ((g, mg._2._2), 1)))
    val genresTagsCount = genresTags.reduceByKey(_ + _)
    val genresTagsCountSorted = genresTagsCount.map(g => (g._1._1, (g._1._2, g._2))).sortByKey(true)

    genresTagsCountSorted.saveAsTextFile(outputURL + "genres-tags-count")

    println("\nMain tags for each genre:")
    val genresTagsCountSortedTop5 = genresTagsCountSorted.groupByKey().map(g => (g._1, g._2.toList.sortBy(_._2).reverse.take(5))).collect
    genresTagsCountSortedTop5.foreach { g =>
      println("\n" + g._1 + " top 5 tags:")
      g._2.foreach(t => println(t._1 + " with " + t._2 + " occurrences"))
    }
  }
}