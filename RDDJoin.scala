import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Exercise1 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","mintemp")
  
  val chapters = sc.textFile("C:/Users/Downloads/Data/chapters.csv")
  val views = sc.textFile("C:/Users/Downloads/Data/views*.csv")
              .map(x => (x.split(',')(1),x.split(',')(0)))
              .filter(x => x._2 != "userId")
              .distinct()
  val titles = sc.textFile("C:/Users/Downloads/Data/titles.csv").map(x => (x.split(',')(0),x.split(',')(1)))             
  
  val chapterArray = chapters.map(x => (x.split(',')(1),1))
  val chapterJoin = chapters.map(x => (x.split(',')(0),x.split(',')(1)))
  val chaptersInCourse = chapterArray.reduceByKey(_+_)
  
  val chaptersCompleted = views.join(chapterJoin).map(x => (x._2,1)).reduceByKey(_+_).map(x => (x._1._2,x._2))
  
  val chapCompletedByTotal = chaptersCompleted.join(chaptersInCourse)
  
  def scoreCalc(score: Float) = {
    
    if (score >= 0.9) 10
    else if (score >= 0.5) 4
    else if (score >= 0.25) 2
    else 0
  }
  
  val result = chapCompletedByTotal.map(x => (x._1,scoreCalc(x._2._1.toFloat/x._2._2.toInt))).reduceByKey(_+_).sortBy(x => x._2,false)
 
  val finalResult = result.join(titles).map(x => (x._2._2,x._2._1))
  
  finalResult.collect.take(100).foreach(println)
  
}
