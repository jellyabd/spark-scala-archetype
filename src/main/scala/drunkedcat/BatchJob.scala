package drunkedcat


import java.util

import cn.com.admaster.infra.mobileutils.MobileDistObjectUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{CombineTextInputFormat, TextInputFormat}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


case class Temp(idAndSpot: String, t: String, ts:Int)


object BatchJob{
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("spark.driver.maxResultSize", "8T")
    val spark = SparkSession
      .builder
      .appName("line count")
      //.master("local")
      .getOrCreate()
    SparkSession.builder().config(new SparkConf)
    import spark.implicits._

    val input = args(0)
    val output = args(1)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new Path(output)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }


    // when using as  :  val partitionCount = if xxxxx else xxxx,    repartition()  will say error, why ?
    var partitionCount = 400
    if (args.length > 2) {
      partitionCount = Integer.decode(args(2))
    } else {
      //skip
    }
    /*  rdd */
    spark.sparkContext.hadoopConfiguration.setInt("mapreduce.input.fileinputformat.split.maxsize", 256 * 1024 * 1024)
    val files = spark.sparkContext.newAPIHadoopFile(input, classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text], spark.sparkContext.hadoopConfiguration)
    val tNotNull = files.map {
      case (_, lineText) => {
        val line = lineText.toString.trim()
        val parts = line.split(",", -1)
        val id = MobileDistObjectUtils.getMobileId(line)
        val t = if (line.contains("view")) "v" else "c"
        val ts = parts(12).toInt
        val spot = parts(25)
        val lId = id + "__" + spot
        (lId, Temp(lId, t, ts))
      }
    }.groupByKey()
      .flatMap{
        case (id, temps) => {
          val ret = new util.ArrayList[Long]
          val sorted = temps.toList.sortBy(x => x.ts)
          var lastV:Temp = null
          for(s <- sorted){
            if(s.t.equals("v")){// view
              lastV = s
            }else{// click
              if(lastV != null){// get view first
                if(lastV.t.equals("v")){
                  ret.add(s.ts - lastV.ts)
                  lastV = s
                }else{// 2 click, skip
                  //skip
                }
              }
            }
          }
          ret.toArray
        }
      }.saveAsTextFile(output)

    val rrr = fs.create(new Path(output + "/count"))
    rrr.writeBytes("" + tNotNull)
    rrr.close()
    /* */

    /* sql
    spark.sqlContext.read.csv(input).createOrReplaceTempView("track")
    spark.sql("select count(_c1) from track where _c30")
    */

    spark.stop()
  }
}
