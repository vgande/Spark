import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object GroupingAggregation extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
   
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DataFrame Learning")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
      
  val ordersDf = spark.read
  .format("csv")
  .option("path","C:/Users/Downloads/Data/orderdata.csv")
  .option("inferSchema",true)
  .option("header",true)
  .load()
  
  //Using Column Object Expression
  
  ordersDf.groupBy("InvoiceNo", "Country")
  .agg(sum("Quantity").as("TotalQuantity"),
       sum(expr("Quantity * UnitPrice")).as("InvoiceTotal")
      ).show()
 
 //Using String Expression
      
  ordersDf.groupBy("InvoiceNo", "Country")
  .agg(expr("sum(Quantity) as TotalQuantity"),
       expr("sum(Quantity * UnitPrice) as InvoiceTotal")
      ).show()
  
  //Using spark sql
  
  ordersDf.createOrReplaceTempView("invoice")
  
  spark.sql("""select InvoiceNo,
               Country,
               sum(Quantity) as TotalQuantity,
               sum(Quantity * UnitPrice) as InvoiceTotal
               from invoice
               group by InvoiceNo, Country
    """).show()
     
  spark.stop()
  
}
