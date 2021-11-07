import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object WithColumn extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def ageCheck(age:Int):String = {
    if (age>18) "Y" else "N"
  }
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DataFrame Learning")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
      
  val inputDf = spark.read
  .format("csv")
  .option("path","C:/Users/Downloads/Data/dataset1.txt")
  .option("inferSchema",true)
  .load()
  
  val formattedDf = inputDf.toDF("name","age","city")
  
  //Register UDF
  /*val parseAgeFunction = udf(ageCheck(_:Int):String)
  formattedDf.withColumn("AdultFlag", parseAgeFunction(col("age"))).show()*/
  
  //Register UDF to catalog
  spark.udf.register("ageCheckFunc",ageCheck(_:Int):String)
  formattedDf.withColumn("AdultFlag",expr("ageCheckFunc(age)")).show()
  
}

spark.stop()
