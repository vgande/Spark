import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object AvgNumberOfFriendsByAge extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","avgfriendsbyage")
  
  val input = sc.textFile("C:\\Users\\Downloads\\Data\\friendsdata.csv")
  
  val splitInput = input.map(x => (x.split("::")(2).toInt,x.split("::")(3).toInt))
  
  val mappedValues = splitInput.mapValues(x => (x,1))
  
  val totalFriendsByAge = mappedValues.reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
  
  val result = totalFriendsByAge.mapValues(x => x._1/x._2).sortBy(x => x._2,false)
  
  result.collect.foreach(println)
  
}