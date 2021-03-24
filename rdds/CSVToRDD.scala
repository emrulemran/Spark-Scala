package refre

import org.apache.spark.sql.SparkSession

object CSVToRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("testrdd").master("local[*]").getOrCreate()
    var path = "C:\\Users\\Emran\\Desktop\\customerstest.csv"
    val rddFromFile = spark.sparkContext.textFile(path)
    println(rddFromFile.getClass)
  
    //rddFromFile.collect.foreach(println)
    
  }
}
