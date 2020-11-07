package sample
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import sample.{IndianCode, SimpleApp}


object Main {
  def main(arg: Array[String]): Unit = {

    //val India = new IndianCode
    //print("India people counting:"+"\n")
    //print(India.counted)
    //print("\n")
    //print(India.tabled)

    // val simpleApp = new SimpleApp
    // print("Sum of numbs in Array"+"\n")
    // print(simpleApp.sumAr)



    val pageCount = new FirstExercise
    // 1st task
    println("1. Top ten lines: ")
    pageCount.myRDD.take(10).foreach(println(_))

    //2nd task
    println("2. Total lines")
    println(pageCount.myRDD.count())

    //3 task
    println("3. Only english Wikipedia")
    pageCount.enPages.take(30).foreach(println(_))

    //4 task
    println("4. Only english Wikipedia in Tuples")
    pageCount.enPagesTuples.take(30).foreach(println(_))


    //5 task
    println("4. Only english Wikipedia in Tuples sorted by content size")
    pageCount.enPagesTuples.sortBy(_._4, false)
      .take(5).foreach(println(_))

    //6 task
    println("6. Most visited page")
    println(pageCount.MostVisitedPage._2 +" "+ pageCount.MostVisitedPage._3)


    //7 task
    // Not mine: doesn't work even being from the example
    def histogram(pageRdd : RDD[(String, String, Long, Long)], nBins : Int) : RDD[(Long, Int)] = {

      // Fisrt, calculate the bounds (min and max)
      val bounds = pageRdd.map(x => (x._3, x._3))
        .reduce((t1, t2) => (math.min(t1._1, t2._1), math.max(t1._2, t2._2)))

      val histRange = bounds._2-bounds._1
      val binWidth = histRange/nBins

      pageRdd
        .map(t => {
          val numReq = t._3
          ((numReq - bounds._1)/binWidth)*binWidth+bounds._1
        })
        .groupBy(x=>x)
        .map(t => (t._1, t._2.size))
        .sortBy(_._1)
    }

    histogram(pageCount.enPagesTuples, 20).take(10).foreach(println)


/*    // 1st task
    println("1. Top ten lines: ")
    pageCount.df2.printSchema()
    pageCount.df2.show(10)

    // 2nd task
    println("2. Total lines")
    println(pageCount.totalLines)

    //3 task
    println("3. Only english Wikipedia")
    pageCount.enPages.printSchema()
    pageCount.enPages.show(30)*/



    //pagecount.top10.foreach(println)
    //println("TotalLength")
    //println(pagecount.totalLength)

}
}