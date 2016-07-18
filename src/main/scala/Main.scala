import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import CleaningData.Log
/**
  * Created by esdras on 17/07/16.
  */
object Main {
  def main(args: Array[String]): Unit = {

    // TODO replace to get from properties file
    val file = "/home/esdras/Downloads/nasa_access_log_jul95/access_log_Jul95"
    val conf = new SparkConf().setMaster("local").setAppName("log-analysis")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val baseDF = sqlContext.read.text(file)
    val logDF = CleaningData.clean(baseDF);

    //Example: HTTP Status Analysis
    val statusCount = logDF.groupBy(Log.STATUS).count().sort(Log.STATUS).cache();
    statusCount.show(30,false)
  }
}
