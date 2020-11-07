package sample

import org.apache.avro.io.Encoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


class FirstExercise {
  val conf = new SparkConf().setAppName("Manipulations with a file")
  val sc = new SparkContext(conf)
  val sparkSession: SparkSession = SparkSession.builder.master("local")
    .appName("Manipulations with a file").getOrCreate()

  val myRDD: DataFrame = sparkSession.read.text("pagecounts-20100806-030000")


  def totalLines: Long = myRDD.count()

  import sparkSession.implicits._
  val df: Dataset[(String, String, String, String)] = myRDD.map(f =>{
    val elements =f.getString(0).split(" ")
    (elements(0), elements(1), elements(2), elements(3))
  })


  def df2: DataFrame = df.withColumnRenamed("_1", "lang")
                        .withColumnRenamed("_2", "title")
                        .withColumnRenamed("_3", "requests")
                        .withColumnRenamed("_4", "contSize")

  def enPages: Dataset[Row] = df2.filter($"lang" === "en")




  //val table = sparkSession.createDataFrame(top10).toDF("1", "2", "3", "4")



  //val lineLength = myRDD.map(s =>s.length)
  //def totalLength = lineLength.reduce((a, b)=> a + b)
}
