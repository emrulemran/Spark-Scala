package batch15

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

case class Person(person_name: String, person_age: Int)

case class Company(name: String, foundingYear: Int, numEmployees: Int)

object SparkSQLCaseClass {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val personDS = Seq(
      Person("Max", 33),
      Person("Adam", 32),
      Person("Muller", 62)).toDS()
    //    personDS.show()

    val inputSeq = Seq(
      Company("ABC", 1998, 310),
      Company("XYZ", 1983, 904),
      Company("NOP", 2005, 83))

    val df = spark.sparkContext.parallelize(inputSeq).toDF() //.as[Company]

    val companyDS = df.as[Company]
    companyDS.show()
    companyDS.registerTempTable("comp")
    // spark.sql("select * from comp").show()

    // DataSet:
    val nums = Seq(1, 2, 3)
    //    val dataDS = spark.sparkContext.parallelize(nums).toDS()
    //    dataDS.show()

    // DataFrame:
    val dataDF = nums.toDF("slNumber")
    // dataDF.show()

    val data = Seq(
      (0, "hello", 5),
      (1, "world", 5),
      (3, "world", 5))

    val dataDF1 = spark.sparkContext.parallelize(data).toDS()

    // dataDF1.show()
    dataDF1.createOrReplaceTempView("myview")

    spark.sql("SELECT * FROM myview").show()

  }

}
