package com.thoughtworks.exercises.batch

import java.util.Properties

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object NeatTotal {
  def main(args: Array[String]): Unit = {
    val log = LogManager.getLogger(this.getClass)

    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream(s"/application.properties"))
    val baseBucket = properties.getProperty("base_bucket")
    val username = properties.get("username")
    val dataFilesBucket = properties.getProperty("data_files_bucket")

    val ordersBucket = s"$baseBucket/$username/$dataFilesBucket/orders"
    val orderItemsBucket = s"$baseBucket/$username/$dataFilesBucket/orderItems"
    val productsBucket = s"$baseBucket/$username/$dataFilesBucket/products"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - ETL Exercises")
      .getOrCreate()

    val productsDF = spark
      .read
      .option("header", true)
      .option("infer_schema", true)
      .option("delimiter", ";")
      .csv(productsBucket)


    productsDF.show

    val orderItemsDF = spark
      .read
      .option("header", true)
      .option("infer_schema", true)
      .option("delimiter", ";")
      .csv(orderItemsBucket)

    orderItemsDF.show

    val productsDFJoinOrderItemsDF = productsDF
      .join(orderItemsDF, "ProductId")
        .select("Price", "Discount", "Quantity")


    productsDFJoinOrderItemsDF.show(false)

    productsDFJoinOrderItemsDF
      .selectExpr("sum((Price - Discount) * Quantity)")
      .as("NEAT TOTAL")
      .show(false)




  }
}
