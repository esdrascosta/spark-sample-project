package data

import com.typesafe.config.ConfigFactory
import data.analysis.{HTTPStatus, Top100Paths}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load()
    val dataPath = conf.getString("spark-example.nasa_log_file")
    val spark =  SparkSession.builder()
                                .appName("log-analysis")
                                .master("local")
                                .getOrCreate()

    val baseDF = spark.read.text(dataPath)
    val logDF = DataCleaner.clean(baseDF)

    HTTPStatus(logDF)
    Top100Paths(logDF)
  }
}
