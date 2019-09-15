import org.apache.spark._
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Core_spark {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")
    conf.set("spark.sql.crossJoin.enabled", "true")

    // val sc = new SparkContext(conf) //sc = sp.SparkContext(appName = 'myapp' */

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("core spark specialization")
      .getOrCreate() /*getOrCreate() is useful when applications may wish to share
     a SparkContext. So yes, you can use it to share a SparkContext object across Applications */
    import spark.implicits._

    val df = spark.read.format("json")
      .load("C:/Users/New/Downloads/Data science in Action/Spark-The-Definitive-Guide-master/data/flight-data/json")

    df.createOrReplaceTempView("df_table")
    /* df = spark.read.format("json").load("Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")
    df.createOrReplaceTempView("df_table")
    # .createOrReplaceTempView: creates the DataFrame as a SQL temporary view
    # for .createGlobaltempview: are views that are shared among all the sessions */

    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false)))
    /* myManualSchema = StructType([StructField("some", StringType(), True),
                             StructField("col", StringType(), True),
                             StructField("names", LongType(), False)]) */

    val myRows = Seq(Row("Hello", null, 1L)) // myRow = Row("Hello", None, 1)
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDF = spark.createDataFrame(myRDD, myManualSchema) // myDf = spark.createDataFrame([myRow], myManualSchema)
    val myDF_2 = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3") //why do we need to use double () at Seq

    println("****************************************************")
//
//    df.select("DEST_COUNTRY_NAME").show(2) // Python is the same
//    spark.sql("SELECT DEST_COUNTRY_NAME " +
//                      "FROM df_table " +
//                      "LIMIT 2" q).show(3)
//    //-- SELECT DEST_COUNTRY_NAME FROM dftable LIMIT 2

//    df.select( // interchangeable methods
//      df.col("DEST_COUNTRY_NAME"),
//      col("DEST_COUNTRY_NAME"),
//      column("DEST_COUNTRY_NAME"),
//      'DEST_COUNTRY_NAME,
//      $"DEST_COUNTRY_NAME",
//      expr("DEST_COUNTRY_NAME")).show(2)
//    /*df.select(F.expr("DEST_COUNTRY_NAME"),
//          F.col("DEST_COUNTRY_NAME"),
//          F.column("DEST_COUNTRY_NAME")).show(2) */
//
//    df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2) //same
//    df.selectExpr("*", "DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as withinCountry").show(2) // same

    df.withColumn("Added col", lit("完了")).show(3)

    df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
      .show(2)



  }

}
