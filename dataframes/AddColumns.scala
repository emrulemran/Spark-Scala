package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

object AddColumns {
  def main(args: Array[String]): Unit = {

    // a comment

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("sparklearning.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    var data = spark.read.option("header", true).csv("C:\\Users\\Emran\\Desktop\\customerstest.csv").toDF()
    var data1 = data.withColumn("gender", lit("male"))

    var data2 = data1.withColumn("status", lit("USCitizen"))

    //  data2.withColumnRenamed("status", "citizenOrNot").show()

    data2.select("id", "gender").where(data2("name") === "jewel").show(false)

  }
}
