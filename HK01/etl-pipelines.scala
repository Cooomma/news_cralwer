object Hk01NewsExtrator{

}

object AcrcloudServerLogExtractor {
  def main(args: Array[String]): Unit = {
    val mlContext = MLContext.fromArgs(args, "com.kkbox.rdc.etl.extract.DailyExtractor")
    val spark = mlContext.newSparkSession(getClass.getName)
    println(mlContext.pathHelper.getAcrcloudServerLogS3Uri("2017-09-13").toString())
    val extractor = new AcrcloudServerLogExtractor(mlContext)
    if (extractor.conf.rerun) {
      extractor.rerun(spark)
    } else {
      extractor.run(spark)
    }

    spark.stop()
  }
}
