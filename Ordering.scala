package windowfuncs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Ordering {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ordering").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1, "dawood", "HR", 2546),
      (2, "ahmed", "Audit", 6523),
      (3, "john", "IT", 12500),
      (4, "ahmed", "Customs", 1540),
      (5, "marie", "Marketing", 1350),
      (6, "alice", "Supply Chain", 2500),
      (7, "farhin", "Transport", 2800),
      (8, "edward", "Customer Care", 7500),
      (9, "billal", "Supplier", 35000),
      (10, "Sam", "Vendor", 15000))

    val dataRDD = spark.sparkContext.parallelize(data)
    val dataDF = dataRDD.toDF("sl", "name", "dept", "salary")

    dataDF.sort(desc("sl"), asc("salary")).take(10).foreach(println(_))

  }
}
