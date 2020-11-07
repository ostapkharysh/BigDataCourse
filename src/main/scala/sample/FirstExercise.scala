package sample

import org.apache.avro.io.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


class FirstExercise {
  val conf = new SparkConf().setAppName("Manipulations with a file")
  val sc = new SparkContext(conf)
  val sparkSession: SparkSession = SparkSession.builder.master("local")
    .appName("Manipulations with a file").getOrCreate()

  val myRDD: RDD[String] = sc.textFile("pagecounts-20100806-030000")

  val enPages: RDD[String] = myRDD.filter(line => line.split(" ")(0).equals("en"))
  val enPagesTuples = enPages.flatMap(line =>{
    val l = line.split(" ")
    if (l.size == 4){
      val lang = l(0)
      val tit = l(1)
      val req = l(2).toLong
      val contSize = l(3).toLong
      List((lang, tit, req, contSize))
    }
    else null
  })

  val MostVisitedPage: (String, String, Long, Long) =
    enPagesTuples.sortBy(_._4, ascending = false).first()








  /*def totalLines: Long = myRDD.count()

  import sparkSession.implicits._
  val df: Dataset[(String, String, String, String)] = myRDD.map(f =>{
    val elements =f.getString(0).split(" ")
    (elements(0), elements(1), elements(2), elements(3))
  })


  def df2: DataFrame = df.withColumnRenamed("_1", "lang")
                        .withColumnRenamed("_2", "title")
                        .withColumnRenamed("_3", "requests")
                        .withColumnRenamed("_4", "contSize")

  def enPages: Dataset[Row] = df2.filter($"lang" === "en")*/




  //val table = sparkSession.createDataFrame(top10).toDF("1", "2", "3", "4")



  //val lineLength = myRDD.map(s =>s.length)
  //def totalLength = lineLength.reduce((a, b)=> a + b)
}
