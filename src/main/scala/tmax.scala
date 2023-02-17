
import org.apache.spark.sql.SparkSession
import scalafx.application.JFXApp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.renderer.SwingRenderer


object tmax extends JFXApp {
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  
  val tschema = StructType(Array(
      StructField("sid",StringType),
      StructField("date",DateType),
      StructField("mtype",StringType),
      StructField("value",DoubleType)
      ))
    
  val data2022 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("src/main/scala/data/2022.csv").cache()
  val data1972 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("src/main/scala/data/1972.csv").cache()
  
  val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("name", StringType)
      ))
  val stationRDD = spark.sparkContext.textFile("src/main/scala/data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).toDouble
    val lon = line.substring(21, 30).toDouble
    val name = line.substring(41, 71)
    Row(id, lat, lon, name)
  }
  val stations = spark.createDataFrame(stationRDD, sschema).cache()
  
  val tmax2022 = data2022.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin2022 = data2022.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps2022 = tmax2022.join(tmin2022, Seq("sid", "date"))
  val dailyTemp2022 = combinedTemps2022.select('sid, 'date, ('tmax)/10 as "tmx2022")
  val stationTemp2022 = dailyTemp2022.groupBy('sid).agg(avg('tmx2022) as "tmx2022")
  val joinedData2022 = stationTemp2022.join(stations, "sid")

  val tmax1972 = data1972.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin1972 = data1972.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps1972 = tmax1972.join(tmin1972, Seq("sid", "date"))
  val dailyTemp1972 = combinedTemps1972.select('sid, 'date, ('tmax)/10 as "tmx1972")
  val stationTemp1972 = dailyTemp1972.groupBy('sid).agg(avg('tmx1972) as "tmx1972")

  val combinedData = stationTemp1972.join(joinedData2022,"sid").withColumn("diff", 'tmx2022 - 'tmx1972)

  println("Below -4: " + combinedData.filter(col("tmx1972") < -4.0).count() + "--->" + combinedData.filter(col("tmx2022") < -4.0).count())
  println("Between -4 & -2: " + combinedData.filter(col("tmx1972") > -4.0 && col("tmx1972") < -2.0).count() + "--->" + combinedData.filter(col("tmx2022") > -4.0 && col("tmx2022") < -2.0).count())
  println("Between -2 & 0: " + combinedData.filter(col("tmx1972") > -2.0 && col("tmx1972") < 0.0).count() + "--->" + combinedData.filter(col("tmx2022") > -2.0 && col("tmx2022") < 0.0).count())
  println("Between 0 & 2: " + combinedData.filter(col("tmx1972") > 0.0 && col("tmx1972") < 2.0).count() + "--->" + combinedData.filter(col("tmx2022") > 0.0 && col("tmx2022") < 2.0).count())
  println("Between 2 & 4: " + combinedData.filter(col("tmx1972") > 2.0 && col("tmx1972") < 4.0).count() + "--->" + combinedData.filter(col("tmx2022") > 2.0 && col("tmx2022") < 4.0).count())
  println("Between 4 & 6: " + combinedData.filter(col("tmx1972") > 4.0 && col("tmx1972") < 6.0).count() + "--->" + combinedData.filter(col("tmx2022") > 4.0 && col("tmx2022") < 6.0).count())
  println("Between 6 & 8: " + combinedData.filter(col("tmx1972") > 6.0 && col("tmx1972") < 8.0).count() + "--->" + combinedData.filter(col("tmx2022") > 6.0 && col("tmx2022") < 8.0).count())
  println("Between 8 & 10: " + combinedData.filter(col("tmx1972") > 8.0 && col("tmx1972") < 10.0).count() + "--->" + combinedData.filter(col("tmx2022") > 8.0 && col("tmx2022") < 10.0).count())
  println("Between 10 & 12: " + combinedData.filter(col("tmx1972") > 10.0 && col("tmx1972") < 12.0).count() + "--->" + combinedData.filter(col("tmx2022") > 10.0 && col("tmx2022") < 12.0).count())
  println("Between 12 & 14: " + combinedData.filter(col("tmx1972") > 12.0 && col("tmx1972") < 14.0).count() + "--->" + combinedData.filter(col("tmx2022") > 12.0 && col("tmx2022") < 14.0).count())
  println("Between 14 & 16: " + combinedData.filter(col("tmx1972") > 14.0 && col("tmx1972") < 16.0).count() + "--->" + combinedData.filter(col("tmx2022") > 14.0 && col("tmx2022") < 16.0).count())
  println("Between 16 & 18: " + combinedData.filter(col("tmx1972") > 16.0 && col("tmx1972") < 18.0).count() + "--->" + combinedData.filter(col("tmx2022") > 16.0 && col("tmx2022") < 18.0).count())
  println("Between 18 & 20: " + combinedData.filter(col("tmx1972") > 18.0 && col("tmx1972") < 20.0).count() + "--->" + combinedData.filter(col("tmx2022") > 18.0 && col("tmx2022") < 20.0).count())
  println("Between 20 & 22: " + combinedData.filter(col("tmx1972") > 20.0 && col("tmx1972") < 22.0).count() + "--->" + combinedData.filter(col("tmx2022") > 20.0 && col("tmx2022") < 22.0).count())
  println("Between 22 & 24: " + combinedData.filter(col("tmx1972") > 22.0 && col("tmx1972") < 24.0).count() + "--->" + combinedData.filter(col("tmx2022") > 22.0 && col("tmx2022") < 24.0).count())
  println("Between 24 & 26: " + combinedData.filter(col("tmx1972") > 24.0 && col("tmx1972") < 26.0).count() + "--->" + combinedData.filter(col("tmx2022") > 24.0 && col("tmx2022") < 26.0).count())
  println("Between 26 & 28: " + combinedData.filter(col("tmx1972") > 26.0 && col("tmx1972") < 28.0).count() + "--->" + combinedData.filter(col("tmx2022") > 26.0 && col("tmx2022") < 28.0).count())
  println("Between 28 & 30: " + combinedData.filter(col("tmx1972") > 28.0 && col("tmx1972") < 30.0).count() + "--->" + combinedData.filter(col("tmx2022") > 28.0 && col("tmx2022") < 30.0).count())
  println("Between 30 & 32: " + combinedData.filter(col("tmx1972") > 30.0 && col("tmx1972") < 32.0).count() + "--->" + combinedData.filter(col("tmx2022") > 30.0 && col("tmx2022") < 32.0).count())
  println("Between 32 & 34: " + combinedData.filter(col("tmx1972") > 32.0 && col("tmx1972") < 34.0).count() + "--->" + combinedData.filter(col("tmx2022") > 32.0 && col("tmx2022") < 34.0).count())
  println("Between 34 & 36: " + combinedData.filter(col("tmx1972") > 34.0 && col("tmx1972") < 36.0).count() + "--->" + combinedData.filter(col("tmx2022") > 34.0 && col("tmx2022") < 36.0).count())
  println("Between 36 & 38: " + combinedData.filter(col("tmx1972") > 36.0 && col("tmx1972") < 38.0).count() + "--->" + combinedData.filter(col("tmx2022") > 36.0 && col("tmx2022") < 38.0).count())
  println("Between 38 & 40: " + combinedData.filter(col("tmx1972") > 38.0 && col("tmx1972") < 40.0).count() + "--->" + combinedData.filter(col("tmx2022") > 38.0 && col("tmx2022") < 40.0).count())
  //println("Between 40 & 42: " + combinedData.filter(col("tmx1972") > 40.0 && col("tmx1972") < 42.0).count() + "--->" + combinedData.filter(col("tmx2022") > 40.0 && col("tmx2022") < 42.0).count())
  //println("Between 42 & 44: " + combinedData.filter(col("tmx1972") > 42.0 && col("tmx1972") < 44.0).count() + "--->" + combinedData.filter(col("tmx2022") > 42.0 && col("tmx2022") < 44.0).count())
  //println("Between 44 & 46: " + combinedData.filter(col("tmx1972") > 44.0 && col("tmx1972") < 46.0).count() + "--->" + combinedData.filter(col("tmx2022") > 44.0 && col("tmx2022") < 46.0).count())
  //println("Above 46: " + combinedData.filter(col("tmx1972") > 46.0).count() + "--->" + combinedData.filter(col("tmx2022") > 46.0).count())


spark.stop()
}

