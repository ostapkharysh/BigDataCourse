package sample
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
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
    pageCount.df2.printSchema()
    pageCount.df2.show(10)

    // 2nd task
    println("2. Total lines")
    println(pageCount.totalLines)

    //3 task
    println("3. Only english Wikipedia")
    pageCount.enPages.printSchema()
    pageCount.enPages.show(30)



    //pagecount.top10.foreach(println)
    //println("TotalLength")
    //println(pagecount.totalLength)

}
}