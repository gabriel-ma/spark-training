package com.thoughtworks.exercises.batch

import java.util.Properties

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object TopSales {
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
//      .master("local")
      .appName("Data Engineering Capability Development - ETL Exercises")
      .getOrCreate()

    val dfOrdersRaw = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(ordersBucket)

    val dfOrderItemsRaw = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(orderItemsBucket)

    val dfProductsRaw = spark.read
      .option("delimiter", ";")
      .option("header", true)
      .option("infer_schema", true)
      .csv(productsBucket)

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dfOrdersWithItems = dfOrdersRaw
      .join(dfOrderItemsRaw, "OrderId")
      .as("ooi")
      .join(dfProductsRaw.as("p"), col("ooi.ProductId") === col("p.ProductId"))

    val totals = dfOrdersWithItems
      .groupBy($"ooi.ProductId", $"p.Name")
      .agg(sum($"ooi.Quantity" ).as("totalQty"),
        sum(($"p.Price" - $"ooi.Discount") * $"ooi.Quantity").as("totalRevenue"))
      .select($"Name", $"totalQty", $"totalRevenue")
      .orderBy($"totalQty".desc)
      .limit(10)
      .collect()
      .map(x => (x.getAs[String](0), x.getAs[Integer](1), x.getAs[Double](2)))

    val locale = new java.util.Locale("pt", "BR")
    val integerFormatter = java.text.NumberFormat.getIntegerInstance(locale)
    val currencyFormatter = java.text.NumberFormat.getCurrencyInstance(locale)
    val totalsFormatted = totals.map(x => (x._1, integerFormatter.format(x._2), currencyFormatter.format(x._3)))

    totalsFormatted.foreach(x => log.info(s"A quantidade vendida do produto ${x._1} foi ${x._2} e o total foi ${x._3}"))
    totalsFormatted.foreach(x => println(s"A quantidade vendida do produto ${x._1} foi ${x._2} e o total foi ${x._3}"))
//    A quantidade vendida do produto TelevisÃ£o foi 24.762.835 e o total foi R$ 5.227.589.970,04
//    A quantidade vendida do produto Vinho foi 24.751.372 e o total foi R$ 1.512.336.045,43
//    A quantidade vendida do produto Camisa foi 24.751.211 e o total foi R$ 1.265.075.068,45
//    A quantidade vendida do produto Bicicleta foi 24.751.164 e o total foi R$ 5.225.359.144,04
//    A quantidade vendida do produto Celular foi 24.750.597 e o total foi R$ 27.501.026.170,46
//    A quantidade vendida do produto Bola de Futebol foi 24.735.046 e o total foi R$ 769.351.599,62
//    A quantidade vendida do produto FogÃ£o foi 24.729.233 e o total foi R$ 7.941.224.984,90
//    A quantidade vendida do produto Geladeira foi 24.728.783 e o total foi R$ 54.875.805.242,88
//    A quantidade vendida do produto CalÃ§a foi 24.725.656 e o total foi R$ 1.980.631.822,30
//    A quantidade vendida do produto Videogame foi 24.716.903 e o total foi R$ 79.371.650.697,30
  }
}
