package data.analysis

import org.apache.spark.sql.DataFrame

/**
  * Created by esdras on 28/09/16.
  */
class TopTwentyFive404ResponseCodeHosts(dataFrame: DataFrame) extends Analyzable {
  override def analyzeAndGenFile: Unit = ???

  override def generatePlotFile: Unit = ???
}
