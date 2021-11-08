import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowAggregation extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
   
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DataFrame Learning")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
      
  val windowDf = spark.read
  .format("csv")
  .option("path","C:/Users/Downloads/Data/windowdata.csv")
  .option("inferSchema",true)
  .load()
  .toDF("Country","weeknum","invoicenum","invoiceqty","invoicetotal")
  
  val myWindow = Window.partitionBy("Country")
  .orderBy("weeknum")
  .rangeBetween(Window.unboundedPreceding, Window.currentRow)
  
  windowDf.withColumn("RunningTotal", sum("invoicetotal").over(myWindow)).show()
  
}
