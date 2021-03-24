package refre

import org.apache.spark.sql.SparkSession

object RDDFromTextFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("testrdd").master("local[*]").getOrCreate()

    var filePath = "C:\\Users\\Emran\\Desktop\\textFiles\\*.txt"

    val rddFromTextFiles = spark.sparkContext.textFile(filePath)
    rddFromTextFiles.foreach(file => {
      println(file)
    })
  }
}
