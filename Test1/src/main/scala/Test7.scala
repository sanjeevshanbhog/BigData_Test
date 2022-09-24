import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test7 {
  
  def main(args:Array[String]):Unit={
  
    val conf = new SparkConf()
                    .setAppName("complexdata")
                    .setMaster("local[*]")
    val sc = new SparkContext(conf)
                    .setLogLevel("ERROR")
                    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val df = spark
                  .read
                  .format("json")
                  .option("multiline","true")
                  .load("file:///C:/Users/sanje/Documents/IT languages/Bigdata/New folder/Data/pets.json")
    
   df.show()
   df.printSchema()
   
   // for expr import sql.function._
   val flattendf = df.withColumn("Permanent address",expr("Address.`Permanent address`"))
                     .withColumn("current Address", expr("Address.`current Address`"))
                     .withColumn("Pets", explode(col("Pets")))
                     .drop("Address")
                      
   flattendf.show()
   flattendf.printSchema()
    
  }
  
}