package data.analysis

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.DataFrame

/**
  * Created by esdras on 27/09/16.
  */
trait Analyzable {
    def analyzeAndGenFile: Unit
    def generatePlotFile: Unit
    def merge(srcPath: String , dstPath: String): Unit = {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    }
}
