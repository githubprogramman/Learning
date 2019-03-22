
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object BaseTest {
  val logger: Logger = LoggerFactory.getLogger(BaseTest.getClass)

  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("BaseTest")

    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    val result = sc.textFile("C:\\Users\\Dell\\Desktop\\日志分析\\error-log24-2.log").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false)

    //停止sc，结束该任务
    result.collect().foreach(println(_))

    //result.saveAsTextFile("hdfs")

    logger.info("----complete!----")

    sc.stop()
  }
}
