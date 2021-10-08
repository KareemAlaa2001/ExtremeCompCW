package imdb

import scala.io.Source


case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  def extractStringListFromPath[K](path: String, parseFunc: String => K): List[K] = {
    val bufferedSource = Source.fromFile(path)
    val list = bufferedSource.getLines().toList.map(line => parseFunc(line))
    bufferedSource.close()
    list
  }

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsList: List[TitleBasics] = extractStringListFromPath(ImdbData.titleBasicsPath, ImdbData.parseTitleBasics)

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsList: List[TitleRatings] =  extractStringListFromPath(ImdbData.titleRatingsPath, ImdbData.parseTitleRatings)


  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewList: List[TitleCrew] = extractStringListFromPath(ImdbData.titleCrewPath, ImdbData.parseTitleCrew)


  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsList: List[NameBasics] = extractStringListFromPath(ImdbData.nameBasicsPath, ImdbData.parseNameBasics)

  def constructSingleGenreTitleBasicsObject(oldTitleBasics: TitleBasics, genre: String): TitleBasics = {
    oldTitleBasics.copy(genres=Some(List(genre)))
  }

  def constructTitleBasicsListForAllGenresFromSingleObject(titleBasics: TitleBasics): List[TitleBasics] = {
    assert(titleBasics.genres.isDefined)
    val genresList: List[String] = titleBasics.genres.get
    genresList.foldRight(List[TitleBasics]())((genre, acc) => titleBasics.copy(genres=Some(List(genre))) :: acc)
  }

  def average(s: List[Int]): Float = {
    s.foldLeft((0f, 1f)) { case ((avg, id), curr) => (avg + (curr - avg)/id, id + 1) }._1
  }

  def getAverageMinMaxRuntimesForTitleBasicsListAndGenre(titleBasicsList: List[TitleBasics], genre: String): (Float, Int, Int, String) = {
    val runtimeMinutes: List[Int] = titleBasicsList.map(_.runtimeMinutes.get)
    val averageRuntime: Float = runtimeMinutes.sum.toFloat / runtimeMinutes.length.toFloat
    (averageRuntime, runtimeMinutes.min, runtimeMinutes.max, genre)
  }

  /*
    Calculate the average, minimum, and maximum runtime duration for all titles per movie genre.
    Note that a title can have more than one genre, thus it should be considered for all of them. 
    The results should be kept in minutes and titles with 0 runtime duration are valid and should be accounted for in your solution.
  */

  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {
    //  starting with the title basics list, need to sort out a bunch of transformations to get to the desired shape
    //  first filter out none values
    //  then flatmap such that each titlebasic object is mapped to a list of titlebasic objects, each with a different genre. could do a fold?
    //  then groupby genre - NOTE: need to account for none genres, single genres and multiple genres.
    //  then for each group, run by
    val noneGenresAndRuntimesRemoved = list.filter(_.genres.isDefined).filter(_.runtimeMinutes.isDefined)
    val flatMapped: List[TitleBasics] = noneGenresAndRuntimesRemoved.flatMap(titleBasics => constructTitleBasicsListForAllGenresFromSingleObject(titleBasics))
    val groupedByGenre: Map[String, List[TitleBasics]] = flatMapped.groupBy[String](_.genres.get.head)
    groupedByGenre.map{case (genre, titleBasicsList) => getAverageMinMaxRuntimesForTitleBasicsListAndGenre(titleBasicsList, genre)}.toList
  }

  /*
    Return the titles of the movies which were released between 1990 and 2018 (inclusive), have an average rating of 7.5 or more, and have received 500000 votes or more.
    For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’.
  */

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    null
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
    null
  }

  // Hint: There could be an input list that you do not really need in your implementation.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {
    null
  }

  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsList))
    // val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    // val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    // val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
    println(durations)
    // println(titles)
    // println(topRated)
    // println(crews)
    println(timing)
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
