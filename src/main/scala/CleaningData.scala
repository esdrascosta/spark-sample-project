import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
/**
  * Created by esdras on 17/07/16.
  */
object CleaningData {

  object Log extends Enumeration {
    type Log = Value
    val HOST = Value("host")
    val TIME = Value("time")
    val TIME_STAMP = Value("timestamp")
    val PATH = Value("path")
    val STATUS = Value("status")
    val CONTENT_SIZE = Value("content_size")

    implicit def toColumnName(column: Log):String ={
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

  //TODO
  private def parceTime(dataFrame: DataFrame): DataFrame = {

    val monthMap = Map(
      "Jan" -> 1, "Feb"-> 2, "Mar" ->3, "Apr" -> 4, "May" ->5 , "Jun" -> 6, "Jul" -> 7,
      "Aug" -> 8, "Sep"-> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12
    )

    dataFrame
  }



  def clean(dataFrame: DataFrame): DataFrame = {
      val extracedColumns = extractColumns(dataFrame)
      val fixedContentSize = fixContentSize(extracedColumns)
      parceTime(fixedContentSize)
  }
}
