
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

  val joinedpastDf = stationTmin72.join(stationTmin73, "sid").join(stationTmin74, "sid").join(stationTmin75, "sid").join(stationTmin76, "sid").join(stationTmax72, "sid").join(stationTmax73, "sid").join(stationTmax74, "sid").join(stationTmax75, "sid").join(stationTmax76, "sid").join(stations, "sid")
  val joinednowDf = stationTmin22.join(stationTmin21, "sid").join(stationTmin20, "sid").join(stationTmin19, "sid").join(stationTmin18, "sid").join(stationTmax22, "sid").join(stationTmax21, "sid").join(stationTmax20, "sid").join(stationTmax19, "sid").join(stationTmax18, "sid").join(stations, "sid")

  //joinedmaxpastDf.show()

  val combinedTemps2022 = joinednowDf.select('sid, 'name, 'lat, 'lon, ('tmax22 + 'tmax21 + 'tmax20 + 'tmax19 + 'tmax18 - 'tmin22 - 'tmin21 - 'tmin20 - 'tmin19 - 'tmin18)/50 as "tdi2022")
  val combinedTemps1972 = joinedpastDf.select('sid, ('tmax72 + 'tmax73 + 'tmax74 + 'tmax75 + 'tmax76 - 'tmin72 - 'tmin73 - 'tmin74 - 'tmin75 - 'tmin76)/50 as "tdi1972")

  val combinedData = combinedTemps1972.join(combinedTemps2022, "sid").select('sid, 'name, 'lat, 'lon, 'tdi2022, 'tdi1972)
  
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

