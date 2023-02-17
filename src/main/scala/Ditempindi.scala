
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


object Ditempindi extends JFXApp {
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
  val dailyTemp2022 = combinedTemps2022.select('sid, 'date, ('tmax - 'tmin)/10 as "tdi2022")
  val stationTemp2022 = dailyTemp2022.groupBy('sid).agg(avg('tdi2022) as "tdi2022")
  val joinedData2022 = stationTemp2022.join(stations, "sid")

  val tmax1972 = data1972.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin1972 = data1972.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps1972 = tmax1972.join(tmin1972, Seq("sid", "date"))
  val dailyTemp1972 = combinedTemps1972.select('sid, 'date, ('tmax - 'tmin)/10 as "tdi1972")
  val stationTemp1972 = dailyTemp1972.groupBy('sid).agg(avg('tdi1972) as "tdi1972")

  val combinedData = stationTemp1972.join(joinedData2022,"sid").withColumn("vari", 'tdi2022 - 'tdi1972)
  
  println("Between 0 & 1: " + combinedData.filter(col("tdi1972") > 0.0 && col("tdi1972") < 1.0).count() + "--->" + combinedData.filter(col("tdi2022") > 0.0 && col("tdi2022") < 1.0).count())
  println("Between 1 & 2: " + combinedData.filter(col("tdi1972") > 1.0 && col("tdi1972") < 2.0).count() + "--->" + combinedData.filter(col("tdi2022") > 1.0 && col("tdi2022") < 2.0).count())
  println("Between 2 & 3: " + combinedData.filter(col("tdi1972") > 2.0 && col("tdi1972") < 3.0).count() + "--->" + combinedData.filter(col("tdi2022") > 2.0 && col("tdi2022") < 3.0).count())
  println("Between 3 & 4: " + combinedData.filter(col("tdi1972") > 3.0 && col("tdi1972") < 4.0).count() + "--->" + combinedData.filter(col("tdi2022") > 3.0 && col("tdi2022") < 4.0).count())
  println("Between 4 & 5: " + combinedData.filter(col("tdi1972") > 4.0 && col("tdi1972") < 5.0).count() + "--->" + combinedData.filter(col("tdi2022") > 4.0 && col("tdi2022") < 5.0).count())
  println("Between 5 & 6: " + combinedData.filter(col("tdi1972") > 5.0 && col("tdi1972") < 6.0).count() + "--->" + combinedData.filter(col("tdi2022") > 5.0 && col("tdi2022") < 6.0).count())
  println("Between 6 & 7: " + combinedData.filter(col("tdi1972") > 6.0 && col("tdi1972") < 7.0).count() + "--->" + combinedData.filter(col("tdi2022") > 6.0 && col("tdi2022") < 7.0).count())
  println("Between 7 & 8: " + combinedData.filter(col("tdi1972") > 7.0 && col("tdi1972") < 8.0).count() + "--->" + combinedData.filter(col("tdi2022") > 7.0 && col("tdi2022") < 8.0).count())
  println("Between 8 & 9: " + combinedData.filter(col("tdi1972") > 8.0 && col("tdi1972") < 9.0).count() + "--->" + combinedData.filter(col("tdi2022") > 8.0 && col("tdi2022") < 9.0).count())
  println("Between 9 & 10: " + combinedData.filter(col("tdi1972") > 9.0 && col("tdi1972") < 10.0).count() + "--->" + combinedData.filter(col("tdi2022") > 9.0 && col("tdi2022") < 10.0).count())
  println("Between 10 & 11: " + combinedData.filter(col("tdi1972") > 10.0 && col("tdi1972") < 11.0).count() + "--->" + combinedData.filter(col("tdi2022") > 10.0 && col("tdi2022") < 11.0).count())
  println("Between 11 & 12: " + combinedData.filter(col("tdi1972") > 11.0 && col("tdi1972") < 12.0).count() + "--->" + combinedData.filter(col("tdi2022") > 11.0 && col("tdi2022") < 12.0).count())
  println("Between 12 & 13: " + combinedData.filter(col("tdi1972") > 12.0 && col("tdi1972") < 13.0).count() + "--->" + combinedData.filter(col("tdi2022") > 12.0 && col("tdi2022") < 13.0).count())
  println("Between 13 & 14: " + combinedData.filter(col("tdi1972") > 13.0 && col("tdi1972") < 14.0).count() + "--->" + combinedData.filter(col("tdi2022") > 13.0 && col("tdi2022") < 14.0).count())
  println("Between 14 & 15: " + combinedData.filter(col("tdi1972") > 14.0 && col("tdi1972") < 15.0).count() + "--->" + combinedData.filter(col("tdi2022") > 14.0 && col("tdi2022") < 15.0).count())
  println("Between 15 & 16: " + combinedData.filter(col("tdi1972") > 15.0 && col("tdi1972") < 16.0).count() + "--->" + combinedData.filter(col("tdi2022") > 15.0 && col("tdi2022") < 16.0).count())
  println("Between 16 & 17: " + combinedData.filter(col("tdi1972") > 16.0 && col("tdi1972") < 17.0).count() + "--->" + combinedData.filter(col("tdi2022") > 16.0 && col("tdi2022") < 17.0).count())
  println("Between 17 & 18: " + combinedData.filter(col("tdi1972") > 17.0 && col("tdi1972") < 18.0).count() + "--->" + combinedData.filter(col("tdi2022") > 17.0 && col("tdi2022") < 18.0).count())
  println("Between 18 & 19: " + combinedData.filter(col("tdi1972") > 18.0 && col("tdi1972") < 19.0).count() + "--->" + combinedData.filter(col("tdi2022") > 18.0 && col("tdi2022") < 19.0).count())
  println("Between 19 & 20: " + combinedData.filter(col("tdi1972") > 19.0 && col("tdi1972") < 20.0).count() + "--->" + combinedData.filter(col("tdi2022") > 19.0 && col("tdi2022") < 20.0).count())
  println("Between 20 & 21: " + combinedData.filter(col("tdi1972") > 20.0 && col("tdi1972") < 21.0).count() + "--->" + combinedData.filter(col("tdi2022") > 20.0 && col("tdi2022") < 21.0).count())
  println("Between 21 & 22: " + combinedData.filter(col("tdi1972") > 21.0 && col("tdi1972") < 22.0).count() + "--->" + combinedData.filter(col("tdi2022") > 21.0 && col("tdi2022") < 22.0).count())
  println("Between 22 & 23: " + combinedData.filter(col("tdi1972") > 22.0 && col("tdi1972") < 23.0).count() + "--->" + combinedData.filter(col("tdi2022") > 22.0 && col("tdi2022") < 23.0).count())
  println("Between 23 & 24: " + combinedData.filter(col("tdi1972") > 23.0 && col("tdi1972") < 24.0).count() + "--->" + combinedData.filter(col("tdi2022") > 23.0 && col("tdi2022") < 24.0).count())
  println("Between 24 & 27: " + combinedData.filter(col("tdi1972") > 24.0 && col("tdi1972") < 27.0).count() + "--->" + combinedData.filter(col("tdi2022") > 24.0 && col("tdi2022") < 27.0).count())


spark.stop()
}

