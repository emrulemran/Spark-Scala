package windowfuncs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object RowNumberDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("windofunctions").master("local[*]").getOrCreate()

    import spark.implicits._

    val dataDF = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)).toDF("employee_name", "department", "salary")
    // dataDF.show()
    //row_number
    val windowSpec = Window.partitionBy("department").orderBy("salary")
    dataDF.withColumn("row_number", row_number.over(windowSpec)).show()
  }
}
