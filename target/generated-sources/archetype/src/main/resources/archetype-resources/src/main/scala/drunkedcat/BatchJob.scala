package drunkedcat


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{CombineTextInputFormat, TextInputFormat}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession




object BatchJob {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("line count")
      //.master("local")
      .getOrCreate()
    SparkSession.builder().config(new SparkConf)

    val input = args(0)
    val output = args(1)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new Path(output)
    if(fs.exists(path)){
      fs.delete(path, true)
    }

    
    // when using as  :  val partitionCount = if xxxxx else xxxx,    repartition()  will say error, why ?
    var partitionCount = 400
    if(args.length > 2){
      partitionCount = Integer.decode(args(2))
    }else{
      //skip
    }
    /*
    spark.sparkContext.hadoopConfiguration.setInt("mapreduce.input.fileinputformat.split.maxsize", 256 * 1024 * 1024)
    val files = spark.sparkContext.newAPIHadoopFile(input, classOf[CombineTextInputFormat], classOf[LongWritable], classOf[Text], spark.sparkContext.hadoopConfiguration)
    val count = files.map{
      case (offset, line) => {
        line.toString().split(",", -1)(77)
      }
    }.distinct()
      .saveAsTextFile("/tmp/packaged")
    */


    spark.read.json(input).repartition(partitionCount).registerTempTable("re_tmp")
    spark.sql("select m6,m7,m6a,count(*) from re_tmp where (m6 is not null) or (m7 is not null) or (m6a is not null) group by m6,m7,m6a").write.csv(output)
    spark.stop()
  }
}
