package data.analysis

import data.DataCleaner.Log
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class UniqueDailyHosts(dataFrame: DataFrame) extends Analyzable {

  override def analyzeAndGenFile: Unit = {


    dataFrame.select( col(Log.HOST), col(Log.TIME).alias("day"))
            .distinct()
            .groupBy("day")
            .count()

  }

  override def generatePlotFile: Unit = ???
}
