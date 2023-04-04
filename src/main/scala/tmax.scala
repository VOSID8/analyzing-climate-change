
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
  
  val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("name", StringType)
      ))
      
  val stationRDD = spark.sparkContext.textFile("D:/Data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).toDouble
    val lon = line.substring(21, 30).toDouble
    val name = line.substring(41, 71)
    Row(id, lat, lon, name)
  }

  val stations = spark.createDataFrame(stationRDD, sschema).cache()
  
  val tschema = StructType(Array(
      StructField("sid",StringType),
      StructField("date",DateType),
      StructField("mtype",StringType),
      StructField("value",DoubleType)
      ))
    
  val data2022 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/present/2022.csv").cache()
  val data2021 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/present/2021.csv").cache()
  val data2020 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/present/2020.csv").cache()
  val data2019 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/present/2019.csv").cache()
  val data2018 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/present/2018.csv").cache()


  val data1972 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/past/1972.csv").cache()
  val data1973 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/past/1973.csv").cache()
  val data1974 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/past/1974.csv").cache()
  val data1975 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/past/1975.csv").cache()
  val data1976 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("D:/Data/past/1976.csv").cache()

  //date1972.show()

  //now
  val tmin2022 = data2022.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin22")
  val tmin2021 = data2021.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin21")
  val tmin2020 = data2020.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin20")
  val tmin2019 = data2019.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin19")
  val tmin2018 = data2018.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin18")
  val tmin1972 = data1972.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin72")
  val tmin1973 = data1973.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin73")
  val tmin1974 = data1974.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin74")
  val tmin1975 = data1975.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin75")
  val tmin1976 = data1976.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin76")

  val stationTmin22 = tmin2022.groupBy('sid).agg(avg('tmin22) as "tmin22")
  val stationTmin21 = tmin2021.groupBy('sid).agg(avg('tmin21) as "tmin21")
  val stationTmin20 = tmin2020.groupBy('sid).agg(avg('tmin20) as "tmin20")
  val stationTmin19 = tmin2019.groupBy('sid).agg(avg('tmin19) as "tmin19")
  val stationTmin18 = tmin2018.groupBy('sid).agg(avg('tmin18) as "tmin18")
  val stationTmin72 = tmin1972.groupBy('sid).agg(avg('tmin72) as "tmin72")
  val stationTmin73 = tmin1973.groupBy('sid).agg(avg('tmin73) as "tmin73")
  val stationTmin74 = tmin1974.groupBy('sid).agg(avg('tmin74) as "tmin74")
  val stationTmin75 = tmin1975.groupBy('sid).agg(avg('tmin75) as "tmin75")
  val stationTmin76 = tmin1976.groupBy('sid).agg(avg('tmin76) as "tmin76")
  
  //tmax
  val tmax1972 = data1972.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax72")
  val tmax1973 = data1973.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax73")
  val tmax1974 = data1974.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax74")
  val tmax1975 = data1975.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax75")
  val tmax1976 = data1976.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax76")
  val tmax2022 = data2022.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax22")
  val tmax2021 = data2021.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax21")
  val tmax2020 = data2020.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax20")
  val tmax2019 = data2019.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax19")
  val tmax2018 = data2018.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax18")
  
  val stationTmax72 = tmax1972.groupBy('sid).agg(avg('tmax72) as "tmax72") 
  val stationTmax73 = tmax1973.groupBy('sid).agg(avg('tmax73) as "tmax73")
  val stationTmax74 = tmax1974.groupBy('sid).agg(avg('tmax74) as "tmax74")
  val stationTmax75 = tmax1975.groupBy('sid).agg(avg('tmax75) as "tmax75")
  val stationTmax76 = tmax1976.groupBy('sid).agg(avg('tmax76) as "tmax76")
  val stationTmax22 = tmax2022.groupBy('sid).agg(avg('tmax22) as "tmax22")
  val stationTmax21 = tmax2021.groupBy('sid).agg(avg('tmax21) as "tmax21")
  val stationTmax20 = tmax2020.groupBy('sid).agg(avg('tmax20) as "tmax20")
  val stationTmax19 = tmax2019.groupBy('sid).agg(avg('tmax19) as "tmax19")
  val stationTmax18 = tmax2018.groupBy('sid).agg(avg('tmax18) as "tmax18")

  val joinedminDf = stationTmin22.join(stationTmin21, "sid").join(stationTmin20, "sid").join(stationTmin19, "sid").join(stationTmin18, "sid").join(stationTmin72, "sid").join(stationTmin73, "sid").join(stationTmin74, "sid").join(stationTmin75, "sid").join(stationTmin76, "sid").join(stations, "sid")
  val joinedmaxDf = stationTmax72.join(stationTmax73, "sid").join(stationTmax74, "sid").join(stationTmax75, "sid").join(stationTmax76, "sid").join(stationTmax22, "sid").join(stationTmax21, "sid").join(stationTmax20, "sid").join(stationTmax19, "sid").join(stationTmax18, "sid").join(stations, "sid")

  //joinedmaxpastDf.show()

  val combinedData = joinedmaxDf.select('sid, 'name, 'lat, 'lon, ('tmax22 + 'tmax21 + 'tmax20 + 'tmax19 + 'tmax18)/50 as "tmx2022", ('tmax72 + 'tmax73 + 'tmax74 + 'tmax75 + 'tmax76)/50 as "tmx1972").cache()
  // val combinedTemps1972 = tmax1972.join(tmin1972, Seq("sid", "date"))
  // val dailyTemp1972 = combinedTemps1972.select('sid, 'date, ('tmax)/10 as "tmx1972")
  // val stationTemp1972 = dailyTemp1972.groupBy('sid).agg(avg('tmx1972) as "tmx1972")

  // val combinedData = stationTemp1972.join(joinedData2022,"sid").withColumn("diff", 'tmx2022 - 'tmx1972)

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

