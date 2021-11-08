import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object SimpleAggregation extends App{
  
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
  
  ordersDf.select(
      count("*").as("RowCount"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgUnitPrice"),
      countDistinct("InvoiceNo").as("UniqueInvoices")
  ).show()
 
 //Using String Expression
      
  ordersDf.selectExpr(
      "count(*) as RowCount",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgUnitPrice",
      "count(Distinct(InvoiceNo)) as UniqueInvoices"
  ).show()
  
  //Using spark sql
  
  ordersDf.createOrReplaceTempView("invoice")
  
  spark.sql("""select count(*) as RowCount,
               sum(Quantity) as TotalQuantity,
               avg(UnitPrice) as AvgUnitPrice,
               count(Distinct(InvoiceNo)) as UniqueInvoices
               from invoice
    """).show()
     
  spark.stop()
  
}
