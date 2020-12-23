package cn.itcast.aws.dw.emr

import org.apache.spark.sql.SparkSession

/**
  * 作者：传智播客
  * 描述：
  */
object SimpleCsvToJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CsvToJSON").getOrCreate()

    val input = args(0)
    val output = args(1)

    val sourceDF = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "$")
      .load(input)

    sourceDF.write.format("json").save(output)
    spark.close()
  }
}
