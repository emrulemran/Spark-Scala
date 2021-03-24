package refre

import org.apache.spark.sql.SparkSession

object MultipleDirs {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("testrdd").master("local[*]").getOrCreate()

    val myrdd = spark.sparkContext.textFile("C:\\Users\\Emran\\Desktop\\dir1\\*, C:\\Users\\Emran\\Desktop\\dir2\\*")
    
    myrdd.foreach(f => {
      println(f)
    })
  }

}
