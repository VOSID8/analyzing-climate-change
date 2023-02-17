
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


object precipcommon extends JFXApp {
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
  
  val prcp2022 = data2022.filter($"mtype" === "PRCP").limit(10000000).drop("mtype").withColumnRenamed("value", "prcp")
  val dailyPrp2022 = prcp2022.select('sid, 'date, ('prcp)/10 as "pri2022")
  val stationPrp2022 = dailyPrp2022.groupBy('sid).agg(avg('pri2022) as "pri2022")
  val joinedData2022 = stationPrp2022.join(stations, "sid")

  val prcp1972 = data1972.filter($"mtype" === "PRCP").limit(10000000).drop("mtype").withColumnRenamed("value", "prcp")
  val dailyPrp1972 = prcp1972.select('sid, 'date, ('prcp)/10 as "pri1972")
  val stationPrp1972 = dailyPrp1972.groupBy('sid).agg(avg('pri1972) as "pri1972")

  val combinedData = stationPrp1972.join(joinedData2022,"sid").withColumn("diff", 'pri2022 - 'pri1972)
  
  //val lons = combinedData.select('lon).as[Double].collect()
  //val lats = combinedData.select('lat).as[Double].collect()
  
  //val ups = combinedData.filter(col("diff") > 0.0)
  //val lonsups = ups.select('lon).as[Double].collect()
  //val latsups = ups.select('lat).as[Double].collect()
  //val variups = ups.select('diff).as[Double].collect()

  val downs = combinedData.filter(col("diff") < 0.0)
  val lonsdowns = downs.select('lon).as[Double].collect()
  val latsdowns = downs.select('lat).as[Double].collect()
  val varidowns = downs.select('diff).as[Double].collect()

  //println(variups.length)
  println(varidowns.length)
  
  {
    val cg = ColorGradient(0.0 -> BlueARGB)
    val plot2 = Plot.scatterPlot(lonsdowns, latsdowns, title = "Stations with decrease in precip", xLabel = "Longitude", 
        yLabel = "Latitude", symbolSize = 3, cg(varidowns))
    SwingRenderer(plot2, 900, 700)
  }

spark.stop()
}




