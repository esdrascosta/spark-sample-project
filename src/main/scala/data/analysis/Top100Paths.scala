package data.analysis

import java.io.File

import data.DataCleaner.Log
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/**
  * Created by esdras on 27/09/16.
  */
class Top100Paths(dataFrame: DataFrame) extends Analyzable {

  private val file = "result_data/_frequent_hosts.json"
  private val destinationFile= "result_data/frequent_hosts.json"

  override def analyzeAndGenFile: Unit = {
    FileUtil.fullyDelete(new File(file))
    FileUtil.fullyDelete(new File(destinationFile))


    dataFrame
      .groupBy(Log.PATH)
      .count()
      .sort(desc("count"))
      .limit(100)
      .write
      .json(file)

    merge(file, destinationFile)

    FileUtil.fullyDelete(new File(file))
  }

  override def generatePlotFile: Unit = ???
}

object Top100Paths {

  def apply(dataFrame: DataFrame): Unit = new Top100Paths(dataFrame).analyzeAndGenFile
}
