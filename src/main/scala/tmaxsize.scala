
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


object tmaxsize extends JFXApp {
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
  
  //val lons = combinedData.select('lon).as[Double].collect()
  //val lats = combinedData.select('lat).as[Double].collect()
  
  val ups = combinedData.filter(col("diff") > 0.0)
  val lonsups = ups.select('lon).as[Double].collect()
  val latsups = ups.select('lat).as[Double].collect()
  val variups = ups.select('diff).as[Double].collect()

  val downs = combinedData.filter(col("diff") < 0.0)
  val lonsdowns = downs.select('lon).as[Double].collect()
  val latsdowns = downs.select('lat).as[Double].collect()
  val varidowns = downs.select('diff).as[Double].collect()

  println(variups.length)
  println(varidowns.length)


spark.stop()
}



