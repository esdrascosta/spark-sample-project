package data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
package object DataCleaner {

    object Log extends Enumeration {
      type Log = Value
      val HOST = Value("host")
      val TIME = Value("time")
      val TIME_STAMP = Value("timestamp")
      val PATH = Value("path")
      val STATUS = Value("status")
      val CONTENT_SIZE = Value("content_size")

      implicit def toColumnName(column: Log): String = {
        column.toString()
      }
    }


    private def extractColumns(dataFrame: DataFrame): DataFrame = {

      dataFrame.select(regexp_extract(dataFrame("value"), "^([^\\s]+\\s)", 1).alias(Log.HOST),
        regexp_extract(dataFrame("value"), "^.*\\[(\\d\\d/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]", 1).alias(Log.TIME_STAMP),
        regexp_extract(dataFrame("value"), "^.*\"\\w+\\s+([^\\s]+)\\s+HTTP.*\"", 1).alias(Log.PATH),
        regexp_extract(dataFrame("value"), "^.*\"\\s+([^\\s]+)", 1).cast("integer").alias(Log.STATUS),
        regexp_extract(dataFrame("value"), "^.*\\s+(\\d+)$", 1).cast("integer").alias(Log.CONTENT_SIZE)
      )
    }

    private def fixContentSize(dataFrame: DataFrame): DataFrame = {
      dataFrame.na.fill(Map(Log.CONTENT_SIZE.toString -> 0))
    }

    private def cleanTime(str: String): String = {
      "%d"
    }

    //TODO
    private def parceTime(dataFrame: DataFrame): DataFrame = {

      val monthMap = Map(
        "Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7,
        "Aug" -> 8, "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12
      )

      //01/Jul/1995:00:00:01 to

      val udfClean = udf((str: String) => {
        "%04d-%02d-%02d %02d:%02d:%02d"
          .format(
            str.substring(7, 11).toInt,
            monthMap(str.substring(3, 6)),
            str.substring(0, 2).toInt,
            str.substring(12, 14).toInt,
            str.substring(15, 17).toInt,
            str.substring(18, 20).toInt)
      })

      val time = udfClean(dataFrame(Log.TIME_STAMP)).cast("timestamp").alias("time")

      dataFrame.select(dataFrame("*"), time).drop("timestamp")
    }


    def clean(dataFrame: DataFrame): DataFrame = {
      val extracedColumns = extractColumns(dataFrame)
      val fixedContentSize = fixContentSize(extracedColumns)
      parceTime(fixedContentSize)
    }
}