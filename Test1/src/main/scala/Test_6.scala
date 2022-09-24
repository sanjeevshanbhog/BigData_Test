import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession




object Test_6 {
    
  def main(args: Array [String]):Unit={
    
    val conf = new SparkConf().setAppName("Complex0").setMaster("local[*]")  
   
    val sc = new SparkContext(conf).setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
   
   
    val df = spark.read.option("multiline","true").format("json").load("file:///C:/Users/sanje/Documents/IT languages/Bigdata/New folder/Data/reqapi.json")
    
    df.show()
    
    df.printSchema()
    
    val flattendf = df.selectExpr(
        
        "data.*",
        "page",
        "per_page",
        "support.*",
        "total",
        "total_pages"
        
    )
   
    
    flattendf.show()
    flattendf.printSchema()
  }
  
  
  
}