package data.analysis

import data.DataCleaner.Log
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by esdras on 28/09/16.
  */
class AverageNumberOfDailyRequestsPerHost(dataFrame: DataFrame) extends Analyzable {

  override def analyzeAndGenFile: Unit = {

    val dailyHostDF = dataFrame.select( col(Log.HOST), col(Log.TIME).alias("day"))
                        .distinct()
                        .groupBy(col("day"))
                        .count()
    val totalReqPerDayDF = dataFrame.groupBy(dayofmonth(col(Log.TIME).alias("day"))).count()
    val avgDailyReqPerHostDF = totalReqPerDayDF.join(dailyHostDF, totalReqPerDayDF("day") === dailyHostDF("day"))
                                    .select(totalReqPerDayDF("day"), totalReqPerDayDF("count") / dailyHostDF("count").alias("avg_reqs_per_host_per_day"))

  }

  override def generatePlotFile: Unit = ???
}
