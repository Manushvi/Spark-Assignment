package CSV

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RetailCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RetailCSV")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    // 1- Load a dataframe from a csv file (retail csv)
    var retailDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/Resource/RetailAnalytics_cpy.csv")
    retailDF.show()

    // 2- Print schema and make sure first row which is header in csv file is considered
    retailDF.printSchema()

    // 3- cast a column from integer to double (here i take quantity column which is cast into double)
    retailDF = retailDF.withColumn("Quantity", col("Quantity").cast("double"))
    retailDF.show()

    // 4- add a new column which is sum of any 3 numeric columns
    retailDF= retailDF.withColumn("sumOfThreeColumns", expr("Quantity + Discount + Shipping_Cost"))
    retailDF.show()

    // 5- delete any one of column
    retailDF = retailDF.drop("Market")
    retailDF.printSchema()

    // 6- print logical plan and check above steps will be present
    retailDF.explain(true)

    // 7-  print some sample rows
    retailDF.show(5)

    // 8 - move file from given location and again try println sample rows you will get error saying file not found
    //retailDF.show(5)

    // 9- move back file to crct place and cache above dataframe
    // retailDF.cache()  // call an action on a cached DataFrame, Spark will read the data from memory instead of from disk
    // retailDF.show(10)


    // 10- now perform action like count
      val rowCount = retailDF.count()
      println(s"Row count: $rowCount")   // 1811 output

    // 11- move file from given location and again try println sample rows , error wont be thrown now because data is present in memory
    //retailDF.show(10)

   // 12- filter above rows for only 2013.
    val retail2013 = retailDF.filter(year($"Order_Date") === 2013)
    retail2013.show(3)

   // 13- print sum of sales for each ship mode on dataframe
    val sumByShipModeDF = retail2013.groupBy("Ship_Mode")
      .agg(sum("Sales").as("Total_Sales"))
    sumByShipModeDF.show()

    // 14- print sum of sales and discount for each ship mode and category (aggregations => sum sales, sum discount, groupBy => ship mode and category)
    val salesDiscountDF = retail2013.groupBy("Ship_Mode", "Category")
      .agg(sum("Sales").as("Total Sales"),
        sum("Discount").as("Total Discount"))
    salesDiscountDF.show()


    // 15- write above dataframes to a csv file.

    // here i use coalesce because it writes the output to a single file instead of multiple files in a directory.
    // provide a good performance as well.

    salesDiscountDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite) //overwrite the existing file with the same name instead of throwing an exception.
      .option("header", "true")
      .option("delimiter", ",")
      .csv("src/main/Resource/salesDiscount.csv")

    sumByShipModeDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter" , ",")
      .csv("src/main/Resource/sumByShipMode.csv")

  }

}
