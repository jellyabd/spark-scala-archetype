package drunkedcat


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{CombineTextInputFormat, TextInputFormat}

import org.apache.spark.sql.SparkSession



object BatchJob {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("line count")
      //.master("local")
      .getOrCreate()

    val input = "/ad4_prod/view/*/170919"
    spark.sparkContext.hadoopConfiguration.setInt("mapreduce.input.fileinputformat.split.maxsize", 256 * 1024 * 1024)
    val files = spark.sparkContext.newAPIHadoopFile(input, classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text], spark.sparkContext.hadoopConfiguration)
    val count = files.map{
      case (offset, line) => {
        offset
      }
    }.count()

    println("lines is " + count)
    spark.stop()
  }
}
