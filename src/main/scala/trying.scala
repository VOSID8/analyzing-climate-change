import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import swiftvis2.plotting
import swiftvis2.plotting._
import scalafx.application.JFXApp
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import swiftvis2.plotting.renderer.SwingRenderer


object trying extends JFXApp {
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val stations = spark.read.textFile("src/main/scala/Data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).trim.toDouble
    val lon = line.substring(21, 30).trim.toDouble
    val elev = line.substring(31, 37).trim.toDouble
    val name = line.substring(41, 71)
    Station(id, lat, lon, elev, name)
  }.cache()

  val tschema = StructType(Array(
      StructField("sid",StringType),
      StructField("date",DateType),
      StructField("mtype",StringType),
      StructField("value",DoubleType)
      ))

  val data2022 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("src/main/scala/data/1972.csv").cache()

  val tmax2022 = data2022.filter($"mtype" === "TMAX").limit(10000000).drop("mtype").withColumnRenamed("value", "tmax")
  val dailyTemp2022 = tmax2022.select('sid, 'date, ('tmax)/10 as "tmx2022")
  val stationTemp2022 = dailyTemp2022.groupBy('sid).agg(avg('tmx2022) as "tmx2022")
  val joinedData2022 = stationTemp2022.join(stations, "sid")


  val stationsVA = new VectorAssembler().setInputCols(Array("tmx2022")).setOutputCol("Max Temp")
  val stationsWithLoc = stationsVA.transform(joinedData2022)
  //  stationsWithLoc.show()

  val kMeans = new KMeans().setK(10).setFeaturesCol("Max Temp").setPredictionCol("cluster")
  val stationClusterModel = kMeans.fit(stationsWithLoc)

  val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
  stationsWithClusters.show()

  //  println(kMeans.explainParams())
  val clu = stationsWithClusters.select('cluster).as[Int].collect()
  val lons = stationsWithClusters.select('lon).as[Double].collect()
  val lats = stationsWithClusters.select('lat).as[Double].collect()

  //count of stations in each cluster
  stationsWithClusters.groupBy('cluster).count().show()

  {
    implicit val df = stationsWithClusters
   		val cg = ColorGradient(0.0 -> BlueARGB, 10.0 -> RedARGB, 5.0 -> GreenARGB)
   		val plot = Plot.scatterPlot(lons, lats, title = "Stations clustered into 10 zones based on Max Temp(1972)", xLabel = "Longitude", yLabel = "Latitude",
  		symbolSize = 3, symbolColor = cg(clu))
  		SwingRenderer(plot, 1000, 650)
  }


  spark.stop()
}

