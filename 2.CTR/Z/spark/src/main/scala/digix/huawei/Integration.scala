package digix.huawei

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangzhe on 21/5/19.
  */
object Integration {

  val DIR_FILE = "test_data"
  val TRAIN = DIR_FILE + "/train_20190518.csv"
  val USER = DIR_FILE + "/user_info.csv"
  val ADDATA = DIR_FILE +  "/ad_info.csv"
  val CONTENT = DIR_FILE + "/content_info.csv"
  val SEP_NUM = 1

  def main(args: Array[String]): Unit = {
    val trainFile = TRAIN
    val userFile = USER
    val adFile = ADDATA
    val contentFile = CONTENT

    val spark = SparkSession.builder.master("local").appName("Integration").getOrCreate()

    val trainData = spark.read
      .format("csv")
      .option("mode", "DROPMALFORMED")
      .schema(StructType(Array(
        StructField("label", StringType, true),
        StructField("r_uId", StringType, true),
        StructField("r_adId", StringType, true),
        StructField("operTime", StringType, true),
        StructField("siteId", StringType, true),
        StructField("slotId", StringType, true),
        StructField("r_contentId", StringType, true),
        StructField("netType", StringType, true)
      )))
      .load(trainFile)
    val userData = spark.read
      .format("csv")
      .option("mode", "DROPMALFORMED")
      .schema(StructType(Array(
        StructField("uId", StringType, true),
        StructField("age", StringType, true),
        StructField("gender", StringType, true),
        StructField("city", StringType, true),
        StructField("province", StringType, true),
        StructField("phoneType", StringType, true),
        StructField("carrier", StringType, true)
      )))
      .load(userFile)
    val adData = spark.read
      .format("csv")
      .option("mode", "DROPMALFORMED")
      .schema(StructType(Array(
        StructField("adId", StringType, true),
        StructField("billId", StringType, true),
        StructField("primId", StringType, true),
        StructField("creativeType", StringType, true),
        StructField("intertype", StringType, true),
        StructField("spreadAppId", StringType, true)
      )))
      .load(adFile)
    val ad_col = adData.columns
    val contentData = spark.read
      .format("csv")
      .option("mode", "DROPMALFORMED")
      .schema(StructType(Array(
        StructField("contentId", StringType, true),
        StructField("firstClass", StringType, true),
        StructField("secondClass", StringType, true)
      )))
      .load(contentFile)
    val content_col = contentData.columns
    trainData.printSchema()
    userData.printSchema()
    adData.printSchema()
    contentData.printSchema()

    val flatData = trainData.join(userData, userData("uId").equalTo(trainData("r_uId")), "left")
      .join(adData, adData("adId").equalTo(trainData("r_adId")), "left")
      .join(contentData, contentData("contentId").equalTo(trainData("r_contentId")), "left")
    val processed = flatData.drop("r_uId").drop("r_adId").drop("r_contentId")
    print(processed.show())

    processed.repartition(SEP_NUM).write.mode(SaveMode.Ignore)
      .format("csv")
      .option("sep", ",")
      .option("header", "false")
      .save("output")

  }


}
