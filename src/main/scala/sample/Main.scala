package sample
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import sample.IndianCode


object Main {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Main application")
    // val sc = new SparkContext(conf)
    val India = new IndianCode
    // print(India.counted)
    print(India.tabled)

}
}