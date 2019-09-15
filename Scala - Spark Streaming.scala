/*
Ref: Beginning Apache Spark 2: With Resilient Distributed Datasets, Spark SQL, Structured Streaming and Spark Machine Learning library
 */

import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


object streaming1 {
  def main(args: Array[String]): Unit = {

    //    val spark_config = new SparkConf()
    //      .setMaster("local")
    //      .setAppName("config")

    val spark = {
      SparkSession.builder()
        .master("local")
        .appName("Streaming_Jaa")
        .getOrCreate()
    }

    import spark.implicits._

    val base = "C:/Users/New/Downloads/Data science in Action/Data set for spark/"
    val mobileDataDF = spark.read.json(base + "beginning-apache-spark-2-master/chapter6/data/mobile")

    val mobileDataSchema = {
      new StructType() // class StructType
        .add("id", StringType, false)
        .add("action", StringType, false)
        .add("ts", TimestampType, false)
    }

    val mobileSSDF = {
      spark.readStream
        .schema(mobileDataSchema)
        .json(base + "beginning-apache-spark-2-master/chapter6/data/input")
    }

    val actionCountDF = {
      mobileSSDF // class Dataset
        .groupBy(window($"ts", "5 minutes"), $"action") // use ts column with window = 10 min
        .count()
    }

    val windowCountDF = {
      mobileSSDF // class Dataset
        .groupBy(window($"ts", "10 minutes")) // use ts column with window = 10 min
        .count()
    }

    mobileSSDF.isStreaming

    val mobileConsoleSQ1 = {
      actionCountDF.writeStream // class StreamingQueryWrapper
        .format("console") //specify type of out-of-box data sin. in this case is "Console"
        .outputMode("complete")
        .option("truncate", false)
        .start()
//        .awaitTermination()
    }
      val mobileConsoleSQ2 = {
        windowCountDF.writeStream // class StreamingQueryWrapper
          .format("console") //specify type of out-of-box data sin. in this case is "Console"
          .outputMode("complete")
          .option("truncate", false)
          .start()
//          .awaitTermination()
      }


    }
  }



