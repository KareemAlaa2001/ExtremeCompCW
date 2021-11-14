package imdb

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]]) {
  def getGenres(): List[String] = genres.getOrElse(List[String]())
}
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  val conf: SparkConf = new SparkConf()
                          .setMaster("local[*]")
                          .setAppName("hw2")
  val sc: SparkContext = SparkContext.getOrCreate(conf)

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath).map(ImdbData.parseTitleBasics)

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath).map(ImdbData.parseTitleRatings)

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath).map(ImdbData.parseTitleCrew)

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath).map(ImdbData.parseNameBasics)

  /*
  Calculate the average, minimum, and maximum runtime duration for all titles per movie genre.
  Note that a title can have more than one genre, thus it should be considered for all of them.
  The results should be kept in minutes and titles with 0 runtime duration are valid and should be accounted for in your solution.
    return type: RDD[(Float, Int, Int, String)]
              avg runtime:Float
              min runtime:Int
              max runtime:Int
              genre:String

   */

  def task1(rdd: RDD[TitleBasics]): RDD[(Float, Int, Int, String)] = {

    val nonesRemoved : RDD[TitleBasics]= rdd.filter(entry => entry.genres.isDefined && entry.runtimeMinutes.isDefined)
    val genreRuntimes : RDD[(String, Int)] = nonesRemoved.flatMap(titleBasics => titleBasics.genres.get.map(genre => (genre, titleBasics.runtimeMinutes.get)))
    genreRuntimes.groupByKey().map{case (genre, runtimes) => ( runtimes.sum.toFloat / runtimes.size.toFloat, runtimes.min, runtimes.max,genre)}
  }

  //  TODO try pair RDD averaging example from the lectures
//  def task1Alt(rdd: RDD[TitleBasics]): RDD[(Float, Int, Int, String)] = {
//    val nonesRemoved : RDD[TitleBasics]= rdd.filter(entry => entry.genres.isDefined && entry.runtimeMinutes.isDefined)
//    val genreRuntimes : RDD[(String, Int)] = nonesRemoved.flatMap(titleBasics => titleBasics.genres.get.map(genre => (genre, titleBasics.runtimeMinutes.get)))
//    genreRuntimes.reduceByKey(() => ( runtimes.sum.toFloat / runtimes.size.toFloat, runtimes.min, runtimes.max,genre))
//  }

  /*
    Return the titles of the movies which were released between 1990 and 2018 (inclusive), have an average rating of 7.5 or more, and have received 500000 votes or more.
    For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’.
  */
  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {
    val filteredTitleRatings: RDD[(String, TitleRatings)] = l2.filter(tr => tr.averageRating >= 7.5 && tr.numVotes >= 500000).map(tr => tr.tconst -> tr)

    val primaryTitles: RDD[(String, String)] = l1.filter( titleBasics => {
      titleBasics.primaryTitle.isDefined && titleBasics.startYear.isDefined && titleBasics.titleType.isDefined && titleBasics.startYear.get >= 1990 && titleBasics.startYear.get <= 2018 && titleBasics.titleType.get == "movie"
    }).map(tb => tb.tconst -> tb.primaryTitle.get)

    primaryTitles.join(filteredTitleRatings).map(_._2._1)
  }


  /*
  Return the top rated movie of each genre for each decade between 1900 and 1999.
  For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’.
  For calculating the top rated movies use the averageRating field and for the release year use the startYear field.
  The output should be sorted by decade and then by genre.
  For the movies with the same rating and of the same decade, print only the one with the title that comes first alphabetically.
  Each decade should be represented with a single digit, starting with 0 corresponding to 1900-1909.
      return type:
        List[(Int, String, String)]
          decade:Int
          genre:String
          title:String
   */
  def getDecade(titleBasics: TitleBasics): Int = (titleBasics.startYear.get / 10) % 10

  def getTopRatedTitle(ratingTitlePairs: Iterable[(Float, String)]): String = {
    ratingTitlePairs.foldRight[(String, Float)](("",0.0f)){case ((currRating, currTitle), accPair) =>

      if (currRating > accPair._2) {

        (currTitle, currRating)

      } else if (currRating == accPair._2 && currTitle < accPair._1) {

        (currTitle, currRating)

      } else accPair

    }._1
  }

  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, String, String)] = {
    val titleRatingsPairs : RDD[(String, TitleRatings)] = l2.map(tr => tr.tconst -> tr)

    val titleBasicsFilteredPairs : RDD[(String,TitleBasics)] = l1.filter(tb =>
        tb.startYear.isDefined && tb.primaryTitle.isDefined && tb.titleType.isDefined
        && tb.genres.isDefined && tb.startYear.get >= 1900 &&
          tb.startYear.get <= 1999 && tb.titleType.get == "movie").map(tb => tb.tconst -> tb)

    val innerJoined : RDD[(String, (TitleBasics, TitleRatings))] = titleBasicsFilteredPairs.join(titleRatingsPairs)

    val groupedByDecadeGenre : RDD[((Int, String), Iterable[(Float, String)])] = innerJoined.flatMap{case (tconst, (tb, tr)) => tb.getGenres().map(genre => ((getDecade(tb), genre),(tr.averageRating, tb.primaryTitle.get))) }.groupByKey()

    val decadeGenreTopRatedTitlesSorted : RDD[(Int, String, String)] = groupedByDecadeGenre.mapValues(getTopRatedTitle).map(tup => (tup._1._1, tup._1._2, tup._2)).sortBy(trio => (trio._1, trio._2))
    decadeGenreTopRatedTitlesSorted
  }

  /*
    In this task we are interested in all the crew names (primaryName) for whom there are at least two known-
    for films released since the year 2010 up to and including the current year (2021).
    You need to return the crew name and the number of such films.
    return type: RDD[(String, Int)]
        crew name:String
        film count:Int
   */

  // Hint: There could be an input RDD that you do not really need in your implementation.
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {

    val filteredTconsts : RDD[(String, Int)] = l1.filter(tb => tb.startYear.isDefined && tb.startYear.get >= 2010 && tb.startYear.get <= 2021).map(tb => tb.tconst -> 1)

    val tconstCastMembers : RDD[(String, (String, String))] = l3.filter(nb => nb.primaryName.isDefined && nb.knownForTitles.isDefined && nb.knownForTitles.get.size >= 2).flatMap(nb => nb.knownForTitles.get.map((_, (nb.nconst, nb.primaryName.get))))

    val relevantContributions : RDD[(String, ((String, String), Int))] = tconstCastMembers.join(filteredTconsts)

    relevantContributions.map(_._2).reduceByKey(_+_).filter(_._2 >= 2).map(tup => tup._1._2 -> tup._2)

  }

  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val crews = timed("Task 4", task4(titleBasicsRDD, titleCrewRDD, nameBasicsRDD).collect().toList)
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
