
import org.apache.spark.sql.SparkSession
import scalafx.application.JFXApp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.renderer.SwingRenderer


object Ditemp1972 extends JFXApp {
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
  val dailyTemp2022 = combinedTemps2022.select('sid, 'date, ('tmax - 'tmin)/10 as "tave2022")
  val stationTemp2022 = dailyTemp2022.groupBy('sid).agg(avg('tave2022) as "tave2022")
  val joinedData2022 = stationTemp2022.join(stations, "sid")

  val tmax1972 = data1972.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin1972 = data1972.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps1972 = tmax1972.join(tmin1972, Seq("sid", "date"))
  val dailyTemp1972 = combinedTemps1972.select('sid, 'date, ('tmax - 'tmin)/10 as "tave1972")
  val stationTemp1972 = dailyTemp1972.groupBy('sid).agg(avg('tave1972) as "tave1972")

  val combinedData = stationTemp1972.join(joinedData2022,"sid")

  combinedData.show()
  combinedData.schema.printTreeString()
  
  
  val filteredData = combinedData.select('lon, 'lat, 'tave1972,'tave2022).as[(Double, Double, Double,Double)].collect()
  //val redpoints = joinedData2022.filter('tave >= 17.0).select('lon, 'lat, 'tave).as[(Double, Double, Double)].collect()
  //val bluepoints = joinedData2022.filter('tave <= 7.0).select('lon, 'lat, 'tave).as[(Double, Double, Double)].collect()
  //val greenpoints = joinedData2022.filter('tave > 7.0 && 'tave < 17).select('lon, 'lat, 'tave).as[(Double, Double, Double)].collect()
  val sizeredpoints1972 = filteredData.count(_._3 > 16.0)
  val sizebluepoints1972 = filteredData.count(_._3 <= 10.0)
  val sizegreenpoints1972 = filteredData.count(_._3 > 10.0) - sizeredpoints1972

  val sizeredpoints2022 = filteredData.count(_._4 > 16.0)
  val sizebluepoints2022 = filteredData.count(_._4 <= 10.0)
  val sizegreenpoints2022 = filteredData.count(_._4 > 10.0) - sizeredpoints2022
  println(sizeredpoints1972)
  println(sizebluepoints1972)
  println(sizegreenpoints1972)

  println(sizeredpoints2022)
  println(sizebluepoints2022)
  println(sizegreenpoints2022)

  val lons = combinedData.select('lon).as[Double].collect()
  val lats = combinedData.select('lat).as[Double].collect()
  val taves1972 = combinedData.select('tave1972).as[Double].collect()
  val taves2022 = combinedData.select('tave2022).as[Double].collect()

  {
    val cg = ColorGradient(6.0 -> BlueARGB, 10.0 -> GreenARGB, 16.0 -> RedARGB)
    val plot = Plot.scatterPlot(lons, lats, title = "Diurnal Temps 1972", xLabel = "Longitude", 
    yLabel = "Latitude", symbolSize = 3, cg(taves1972))
    SwingRenderer(plot, 800, 600)
  }
  {
    val cg = ColorGradient(6.0 -> BlueARGB, 10.0 -> GreenARGB, 16.0 -> RedARGB)
    val plot = Plot.scatterPlot(lons, lats, title = "Diurnal Temps 2022", xLabel = "Longitude", 
    yLabel = "Latitude", symbolSize = 3, cg(taves2022))
    SwingRenderer(plot, 800, 600)
  }

spark.stop()
}

