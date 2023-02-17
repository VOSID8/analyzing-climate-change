
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


object NOAAData extends JFXApp {
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
  val data1952 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("src/main/scala/data/1952.csv").cache()
  
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

  val tmax1952 = data1952.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin1952 = data1952.filter('mtype === "TMIN").limit(10000000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps1952 = tmax1952.join(tmin1952, Seq("sid", "date"))
  val dailyTemp1952 = combinedTemps1952.select('sid, 'date, ('tmax - 'tmin)/10 as "tave1952")
  val stationTemp1952 = dailyTemp1952.groupBy('sid).agg(avg('tave1952) as "tave1952")

  val combinedData = stationTemp1952.join(joinedData2022,"sid")

  combinedData.show()
  combinedData.schema.printTreeString()
  
  
  val filteredData = combinedData.select('lon, 'lat, 'tave1952,'tave2022).as[(Double, Double, Double,Double)].collect()
  //val redpoints = joinedData2022.filter('tave >= 15.0).select('lon, 'lat, 'tave).as[(Double, Double, Double)].collect()
  //val bluepoints = joinedData2022.filter('tave <= 5.0).select('lon, 'lat, 'tave).as[(Double, Double, Double)].collect()
  //val greenpoints = joinedData2022.filter('tave > 5.0 && 'tave < 15).select('lon, 'lat, 'tave).as[(Double, Double, Double)].collect()
  val sizeredpoints1952 = filteredData.count(_._3 > 15.0)
  val sizebluepoints1952 = filteredData.count(_._3 <= 5.0)
  val sizegreenpoints1952 = filteredData.count(_._3 > 5.0) - sizeredpoints1952

  val sizeredpoints2022 = filteredData.count(_._4 > 15.0)
  val sizebluepoints2022 = filteredData.count(_._4 <= 5.0)
  val sizegreenpoints2022 = filteredData.count(_._4 > 5.0) - sizeredpoints2022
  println(sizeredpoints1952)
  println(sizebluepoints1952)
  println(sizegreenpoints1952)
  println(sizeredpoints2022)
  println(sizebluepoints2022)
  println(sizegreenpoints2022)

  // val lons = joinedData2022.select('lon).as[Double].collect()
  // val lats = joinedData2022.select('lat).as[Double].collect()
  // val taves = joinedData2022.select('tave).as[Double].collect()

  // {
    // val cg = ColorGradient(5.0 -> BlueARGB, 10.0 -> GreenARGB, 15.0 -> RedARGB)
    // val plot = Plot.scatterPlot(lons, lats, title = "Difference Temps 1952", xLabel = "Longitude", 
        // yLabel = "Latitude", symbolSize = 3, cg(taves))
    // SwingRenderer(plot, 600, 500)
  // }

spark.stop()
}
