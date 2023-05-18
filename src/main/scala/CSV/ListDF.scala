package CSV

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, dayofmonth, month, sum, to_date, year}
import org.apache.spark.sql.types.{NumericType, StringType, TimestampType}

object ListDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RetailCSV")
      .master("local")
      .getOrCreate()

    val retailDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/Resource/RetailAnalytics_cpy.csv")
    retailDF.show()
    retailDF.printSchema()

    val numericColumns = retailDF.schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map(_.name).toList
    val stringColumns = retailDF.schema.fields.filter(_.dataType == StringType).map(_.name).toList
    val dateColumns = retailDF.schema.fields.filter(_.dataType == TimestampType).map(_.name).toList

    println(numericColumns)
    println(stringColumns)
    println(dateColumns)


    // 2- create a dataframe with addition 3 columns for each date column from above list => day_${dateCol}, month_${dateCol}, year_${dateCol}

    val dateColumnsDF = dateColumns.foldLeft(retailDF) { (df, dateCol) =>
      df.withColumn(s"day_$dateCol", dayofmonth(to_date(col(dateCol)))) // here, to_date() function is used to convert the timestamp to a date before extracting the day
        .withColumn(s"month_$dateCol", month(to_date(col(dateCol))))
        .withColumn(s"year_$dateCol", year(to_date(col(dateCol))))
    }
    dateColumnsDF.printSchema()
    dateColumnsDF.show()



     // 1- create a dataframe with aggregation where even index columns will have sum as aggregation and odd index cols will have avg,
     // group by will be even indicies of groupby list.

    val aggregations= numericColumns.indices.map { i =>
      if (i % 2 == 0) sum(numericColumns(i)).alias(numericColumns(i) + "_sum")
      else avg(numericColumns(i)).alias(numericColumns(i) + "_avg")
    }
    // The groupBy function creates groups based on the unique combinations of stringColumns and agg function is then applied to each group
    val aggregatedDF = retailDF.groupBy(stringColumns.head, stringColumns.tail: _*).agg(aggregations.head, aggregations.tail: _*)
    aggregatedDF.show()


  }

}
