package imdb

import scala.collection.immutable.ListMap
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
    val noneGenresAndRuntimesRemoved = list.filter(entry => entry.genres.isDefined && entry.runtimeMinutes.isDefined)
    val flatMapped: List[TitleBasics] = noneGenresAndRuntimesRemoved.flatMap(titleBasics => titleBasics.genres.get.map(genre => titleBasics.copy(genres = Some(List(genre)))))
    val groupedByGenre: Map[String, List[TitleBasics]] = flatMapped.groupBy[String](_.genres.get.head)
    groupedByGenre.map{case (genre, titleBasicsList) => getAverageMinMaxRuntimesForTitleBasicsListAndGenre(titleBasicsList, genre)}.toList
  }

  /*
    Return the titles of the movies which were released between 1990 and 2018 (inclusive), have an average rating of 7.5 or more, and have received 500000 votes or more.
    For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’.
  */


  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {

    val titleRatingsTconstMap: Map[String, TitleRatings] = l2.foldLeft(Map[String, TitleRatings]())((acc, titleRatings) => acc + (titleRatings.tconst -> titleRatings))

    l1.filter( titleBasics => {
      titleBasics.primaryTitle.isDefined && titleBasics.startYear.isDefined && titleBasics.titleType.isDefined && titleBasics.startYear.get >= 1990 && titleBasics.startYear.get <= 2018 && titleBasics.titleType.get == "movie"  && titleRatingsTconstMap.contains(titleBasics.tconst) && titleRatingsTconstMap(titleBasics.tconst).averageRating >= 7.5 && titleRatingsTconstMap(titleBasics.tconst).numVotes >= 500000
    }).map(_.primaryTitle.get)
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
  /*
    Class defs (for readability):
          case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
          case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
   */

  def getDecade(titleBasics: TitleBasics): Int = (titleBasics.startYear.get / 10) % 10

  def isEligibleTask3(tb: TitleBasics, titleRatingsTconstMap :  Map[String, TitleRatings]): Boolean = {
    titleRatingsTconstMap.contains(tb.tconst) && tb.startYear.isDefined && tb.primaryTitle.isDefined && tb.titleType.isDefined && tb.genres.isDefined && tb.startYear.get >= 1900 && tb.startYear.get <= 1999 && tb.titleType.get == "movie"
  }

  def getTopRatedTitle(titleBasicsList: List[TitleBasics], titleRatingsTconstMap :  Map[String, TitleRatings]): String = {
    titleBasicsList.foldRight[(String, Float)](("",0.0f))((titleBasics, accPair) => {

      if (titleRatingsTconstMap(titleBasics.tconst).averageRating > accPair._2) {

        (titleBasics.primaryTitle.get, titleRatingsTconstMap(titleBasics.tconst).averageRating)

      } else if (titleRatingsTconstMap(titleBasics.tconst).averageRating == accPair._2 && titleBasics.primaryTitle.get < accPair._1) {

        (titleBasics.primaryTitle.get, titleRatingsTconstMap(titleBasics.tconst).averageRating)

      } else accPair
    })._1
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
    //  need a similar filter situation first. keep only items we know are in both titlebasics and titleratings.
    //  relevant fields: tb.startYear, tb.primaryTitle, tb.titleType, tb.genres, tr.averageRating
    //  filter by fields defined, start year between 1900 and 1999, and movie titletype
    //  will need to group by genre AND group by decade?
    val titleRatingsTconstMap : Map[String, TitleRatings] = l2.foldLeft(Map[String, TitleRatings]())((acc, titleRatings) => acc + (titleRatings.tconst -> titleRatings))

    val titleBasicsFiltered : List[TitleBasics] = l1.filter(isEligibleTask3(_, titleRatingsTconstMap))

    val genreFlatMapped : List[TitleBasics] = titleBasicsFiltered.flatMap(titleBasics => titleBasics.genres.get.map(genre => titleBasics.copy(genres = Some(List(genre)))))

    val groupedByDecade : Map[Int, List[TitleBasics]] = genreFlatMapped.groupBy[Int](getDecade)

    val decadeGenreMap : Map[Int, Map[String, List[TitleBasics]]] = groupedByDecade.mapValues(_.groupBy(_.genres.get.head))

    //  TODO need to sort my maps by decade then genre, converting to the nested pair structure to maintain order.
    //  TODO fold through sorted structure, finding the top rated movie of each decade-genre (implicitly maintaining alphabetical order with comparison)

    val sortedNestedStructure : List[(Int, List[(String, List[TitleBasics])])] = decadeGenreMap.mapValues(_.toList.sortBy(_._1)).toList.sortBy(_._1)
    sortedNestedStructure.flatMap(decadeListPair => (List.fill(decadeListPair._2.length)(decadeListPair._1), decadeListPair._2.map(genreTbListPair => (genreTbListPair._1, getTopRatedTitle(genreTbListPair._2, titleRatingsTconstMap)))).zipped.toList).map{case (decade, (genre, title)) => (decade, genre, title)}
  }

  // Hint: There could be an input list that you do not really need in your implementation.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {
    null
  }

  def main(args: Array[String]) {
//    val durations = timed("Task 1", task1(titleBasicsList))
//    val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
     val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    // val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
//    println(durations)
//    println(titles)
    println(topRated)
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
