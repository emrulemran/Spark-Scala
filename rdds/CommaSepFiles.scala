package refre

import org.apache.spark.sql.SparkSession

object CommaSepFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("testrdd").master("local[*]").getOrCreate()

/*
    var file1 = "C:\\Users\\Emran\\Desktop\\textFiles\\text1.txt"
    var file2 = "C:\\Users\\Emran\\Desktop\\textFiles\\text2.txt"
    var file3 = "C:\\Users\\Emran\\Desktop\\textFiles\\text3.txt"
    var file4 = "C:\\Users\\Emran\\Desktop\\textFiles\\text4.txt"
    var file5 = "C:\\Users\\Emran\\Desktop\\textFiles\\text5.txt"
    var file6 = "C:\\Users\\Emran\\Desktop\\textFiles\\text6.txt"
*/

val rddFromTextFile = spark.sparkContext.textFile("C:\\Users\\Emran\\Desktop\\textFiles\\text1.txt, C:\\Users\\Emran\\Desktop\\textFiles\\text2.txt")
    rddFromTextFile.foreach(file => {
      println(file)
    })

  }
}
