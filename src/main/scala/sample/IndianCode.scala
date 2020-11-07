package sample

import org.apache.spark.sql.SparkSession


class IndianCode {

  val sparkSession: SparkSession = SparkSession.builder.master("local").appName("Scala Spark Example").getOrCreate()
  val csvPO = sparkSession.read.option("inferSchema", true).option("header", true).
    csv("all_india_PO.csv")
  csvPO.createOrReplaceTempView("tabPO")
  def counted: Long = sparkSession.sql("select * from tabPO").count()
  def tabled = sparkSession.sql("select statename as StateName,count(*) " +
    "as TotalPOs from tabPO group by statename order by count(*) desc").show(50)


}
