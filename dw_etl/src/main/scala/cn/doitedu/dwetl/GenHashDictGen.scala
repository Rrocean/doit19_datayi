package cn.doitedu.dwetl

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession



object GenHashDictGen {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("genhash字典生成")
      .master("local[*]")
      .getOrCreate()

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")

    val frame = spark.read.jdbc("jdbc:mysql://hadoop102:3306/realtimedw", "area_dict", props)
    frame.createTempView("df")

    val gps2geohash = (lat: Double, lng: Double) => {
      GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
    }
    spark.udf.register("gps2geohash", gps2geohash)
    val res = spark.sql(
      """
        |select
        |gps2geohash(BD09_LAT,BD09_LNG) as geohash,
        |province,
        |city,
        |region
        |from df
        |
        |
        |""".stripMargin)
    res.write.parquet("hdfs://hadoop102:8020/dicts/geodict")
    res.show(100, false)

  }

}
