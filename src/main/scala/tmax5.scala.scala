
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


object tmax5 extends JFXApp {
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

  val combinedTempsmax = joinedmaxDf.select('sid, 'name, 'lat, 'lon, ('tmax22 + 'tmax21 + 'tmax20 + 'tmax19 + 'tmax18 - 'tmax72 - 'tmax73 - 'tmax74 - 'tmax75 - 'tmax76)/50 as "tavemax")
  val combinedTempsmin = joinedminDf.select('sid, 'name, 'lat, 'lon, ('tmin18 + 'tmin19 + 'tmin20 + 'tmin21 + 'tmin22 - 'tmin72 - 'tmin73 - 'tmin74 - 'tmin75 - 'tmin76)/50 as "tavemin")

  //val combinedData = combinedTemps1972.join(combinedTemps2022, "sid").select('sid, 'name, 'lat, 'lon, ('tave2022 - 'tave1972) as "vari")
  
  val upstmax = combinedTempsmax.filter(col("tavemax") > 0.0)
  val lonsupstmax = upstmax.select('lon).as[Double].collect()
  val latsupstmax = upstmax.select('lat).as[Double].collect()
  val variupstmax = upstmax.select('tavemax).as[Double].collect()

  val downstmax = combinedTempsmax.filter(col("tavemax") < 0.0)
  val lonsdownstmax = downstmax.select('lon).as[Double].collect()
  val latsdownstmax = downstmax.select('lat).as[Double].collect()
  val varidownstmax = downstmax.select('tavemax).as[Double].collect()

  val upstmin = combinedTempsmin.filter(col("tavemin") > 0.0)
  val lonsupstmin = upstmin.select('lon).as[Double].collect()
  val latsupstmin = upstmin.select('lat).as[Double].collect()
  val variupstmin = upstmin.select('tavemin).as[Double].collect()
  
  val downstmin = combinedTempsmin.filter(col("tavemin") < 0.0)
  val lonsdownstmin = downstmin.select('lon).as[Double].collect()
  val latsdownstmin = downstmin.select('lat).as[Double].collect()
  val varidownstmin = downstmin.select('tavemin).as[Double].collect()

  println("Tmax increase: "+variupstmax.length)
  println("Tmax decrease: "+varidownstmin.length)
  println("Tmin increase: "+variupstmin.length)
  println("Tmin decrease: "+varidownstmin.length)

  {
    val cg = ColorGradient(0.0 -> RedARGB)
    val plot1 = Plot.scatterPlot(lonsupstmax, latsupstmax, title = "Stations w/ increase in Max. Temp", xLabel = "Longitude", 
        yLabel = "Latitude", symbolSize = 3, cg(variupstmax))
    SwingRenderer(plot1, 800, 600)
  }
  {
    val cg = ColorGradient(0.0 -> BlueARGB)
    val plot2 = Plot.scatterPlot(lonsdownstmax, latsdownstmax, title = "Stations w/ decrease in Max. Temp", xLabel = "Longitude", 
        yLabel = "Latitude", symbolSize = 3, cg(varidownstmax))
    SwingRenderer(plot2, 800, 600)
  }

  {
    val cg = ColorGradient(0.0 -> RedARGB)
    val plot3 = Plot.scatterPlot(lonsupstmin, latsupstmin, title = "Stations w/ increase in Min. Temp", xLabel = "Longitude", 
        yLabel = "Latitude", symbolSize = 3, cg(variupstmin))
    SwingRenderer(plot3, 800, 600)
  }
  {
    val cg = ColorGradient(0.0 -> BlueARGB)
    val plot4 = Plot.scatterPlot(lonsdownstmin, latsdownstmin, title = "Stations w/ decrease in Min. Temp", xLabel = "Longitude", 
        yLabel = "Latitude", symbolSize = 3, cg(varidownstmin))
    SwingRenderer(plot4, 800, 600)
  }

spark.stop()
}
