package data

import data.analysis.{Top100Paths, HTTPStatus}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // TODO replace to get from properties file
    val data = "/home/esdras/Downloads/nasa_log/access_log_Aug95"
    val spark =  SparkSession.builder()
                                .appName("log-analysis")
                                .master("local")
                                .getOrCreate()

    val baseDF = spark.read.text(data)
    val logDF = DataCleaner.clean(baseDF)

    HTTPStatus(logDF)
    Top100Paths(logDF)
  }
}
