import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object MultipleDelimiters extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  
  case class Order(order_id:Int, order_date:String, cust_id:Int,status:String)
  
  def parser(line: String) = {
    line match {
      case myregex(order_id, order_date, cust_id,status) =>
        Order(order_id.toInt, order_date, cust_id.toInt,status)
    }
  }
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Multiple Delim")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val lines = spark.sparkContext.textFile("C:/Users/Downloads/Data/ordersnew.txt")
  
  import spark.implicits._
  
  val linesDs = lines.map(parser).toDS()
  
  linesDs.printSchema()
  linesDs.show()
  linesDs.groupBy("status").count.show()
  
}
