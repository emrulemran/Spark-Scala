package refre

import org.apache.spark.sql.SparkSession

object EmptyRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("testrdd").master("local[*]").getOrCreate()
    val empRDD = spark.sparkContext.emptyRDD
    println(empRDD)

    println("Number of RDD Partitions: " + empRDD.getNumPartitions) // returns 0 partition

  }
}
