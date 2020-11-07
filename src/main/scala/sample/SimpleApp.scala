package sample

import org.apache.spark.{SparkConf, SparkContext}

class SimpleApp {
  val conf = new SparkConf().setAppName("Main application")
  val sc = new SparkContext(conf)

  val nums = Array((1 to 50):_*)
  val oneRDD = sc.parallelize(nums)
  oneRDD.count
  val otherRDD = oneRDD.map(_ * 2)
  val result = otherRDD.collect
  // for (v <- result) println(v)
  def sumAr: Int = oneRDD.reduce(_ + _)

}
