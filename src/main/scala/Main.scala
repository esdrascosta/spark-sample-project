import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by esdras on 17/07/16.
  */
object Main {
  def main(args: Array[String]): Unit = {

    val file = "/home/esdras/data_analisis/access_log_Jul95.d/access_log_Jul95"
    val conf = new SparkConf().setMaster("local").setAppName("log-analysis")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val baseDF = sqlContext.read.text(file)
    val logDF = CleaningData.clean(baseDF);
    logDF.show(30,false)
  }
}
