import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import java.sql.Timestamp


object DataFrameLearning extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DataFrame Learning")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //Create a schema for the Orders file
  
  val ordersSchema = StructType(List(
      StructField("orderid",IntegerType),
      StructField("orderdate",TimestampType),
      StructField("customerid",IntegerType),
      StructField("status",StringType)
      ))
      
  val ordersSchemaDDL = "orderid Int,orderdate Timestamp,customerid Int,status String"
  
  //Creating a case class for dataframe to dataset conversion
  
  case class ordersSchemaDS (orderid:Int, orderdate:Timestamp, customerid:Int, status:String)
  
  val ordersDf = spark.read
  .format("csv")
  .option("header",true)
  //.option("inferSchema",true) //Not a recommended choice in Production
  //.schema(ordersSchema) //reading the file using the Explicit Schema in Programmatic approach
  .schema(ordersSchemaDDL) //reading the file using the Explicit Schema in DDL approach
  .option("path","C:/Users/venki/Downloads/Data/orders.csv")
  .load
  
  //Dataframe to Dataset. import should be here only
  
  import spark.implicits._
  
  val ordersDs = ordersDf.as[ordersSchemaDS]
  
  ordersDf.show()
  ordersDf.printSchema()
  
  
  val groupedOrdersDf = ordersDf
  .repartition(4)
  .where("customerid > 10000")
  .select("orderid","customerid")
  .groupBy("customerid")
  .count
  
  groupedOrdersDf.show()
  groupedOrdersDf.printSchema()
  
  //Schema is implicitly read by default for json format and no need to Infer
  //Any of the 3 modes can be defined as per business need.
  //PERMISSIVE (Null Values for the Malformed records and an extra column is created with the Malformed record data)
  //DROPMALFORMED (Drop the Malformed Records)
  //FAILFAST (Throw an exception for Malformed Records)
  val playersDf = spark.read
  .format("json")
  .option("path","C:/Users/venki/Downloads/Data/players.json")
  .option("mode","DROPMALFORMED")
  .load
  
  playersDf.show()
  playersDf.printSchema()
  
  //parquet format is default, no need to explicitly mention it
  //schema is part of the file and read by default. No need to Infer
  val usersDf = spark.read
  .option("path","C:/Users/venki/Downloads/Data/users.parquet")
  .load
  
  usersDf.show()
  usersDf.printSchema()
  
  //Logger.getLogger("org").info("Job Completed Successfully")
  
  spark.stop()
  
}
