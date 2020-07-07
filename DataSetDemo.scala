package datasetdemo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Column

case class Person(person_name: String, person_age: Int)

case class Company(name: String, foundingYear: Int, numEmployees: Int)

object DataSetDemo {
  def main(args: Array[String]): Unit = {

    // configuring SparkSession:
    val spark = SparkSession.builder().appName("test sparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._

    val animals = Seq(
      (1, "bat"),
      (2, "mouse"),
      (3, "horse"))

    // create an RDD:
    val animalsRDD = spark.sparkContext.parallelize(animals)
   

   // create a DataFrame with column headers:
   val students = Seq(
      (10, "hasan"),
      (11, "mazhar"),
      (12, "farzam")).toDF("batch", "name")

       val emp = Seq(
      ("hasan", 12500),
      ("mazhar", 20000),
      ("farzam", 15750)) //.toDF("name", "salary")

    val empRDD = spark.sparkContext.parallelize(emp)

    val empTab = empRDD.toDF().registerTempTable("employee")
    // val q = spark.sql.hive.HiveContext("select * from employee")
    //q.collect.foreach(println)

    emp.show()
    val bonus = emp.withColumn("bonus", col("salary") * 0.25)
    bonus.show()

    // val bonusgranted = bonus.select(col("name"), col("salary"), col("bonus"), lit("given").as("bonus given"))
    bonusgranted.show()

    // the first example:
    val dataset = Seq(1, 2, 3).toDS()
    dataset.withColumn("nums", col("value")).show()

    // the second example:
    val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
    personDS.show()

    val compSeq = Seq(
      Company("ABC", 1998, 310),
      Company("XYZ", 1983, 904),
      Company("NOP", 2005, 83))

    val df = spark.sparkContext.parallelize(compSeq).toDF()
    val companyDS = df.as[Company]
    companyDS.registerTempTable("comp")
    val all = spark.sql("select * from comp where foundingYear=2005")
    all.collect().foreach(println)

    // DS from RDD:
    val rdd = spark.sparkContext.parallelize(Seq((1, "Spark"), (2, "Java")))
    val integerDS = rdd.toDS()
    integerDS.show()
  }
}
