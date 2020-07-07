package defsdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SimpleSorting {
  def main(args: Array[String]): Unit = {
    val seq = Seq(12, 3, 78, 90, 1, 454, 65, 232, 98)
    println("Sorted ascending: " + seq.sorted)
    println("Sorted descending: " + seq.sorted(Ordering.Int.reverse))

    val spark = SparkSession.builder().appName("abc").master("local[*]").getOrCreate()
    import spark.implicits._

    val dataDF = spark.sparkContext.parallelize(seq).toDF("values")
    dataDF.show()
    dataDF.sort(desc("values")).take(10).foreach(println(_))

  }
}
