package sample
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import sample.{IndianCode, SimpleApp}


object Main {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Main application")
    //val sc = new SparkContext(conf)
    val India = new IndianCode
    print("India people counting:"+"\n")
    print(India.counted)
    print("\n")
    print(India.tabled)

    // val simpleApp = new SimpleApp
    // print("Sum of numbs in Array"+"\n")
    // print(simpleApp.sumAr)
}
}