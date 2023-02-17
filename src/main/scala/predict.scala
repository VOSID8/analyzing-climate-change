import org.apache.spark.sql.SparkSession
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


/*
 * NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/  in the by_year directory
 */

case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)
case class NOAADataa(sid: String, date: java.sql.Date, measure: String, value: Double)
case class ClusterData(num: Int, lat: Double, lon: Double, latstd: Double, lonstd: Double,
  tmax: Double, tmin: Double, tmaxstd: Double, tminstd: Double, precip: Double,
  tmaxSeasonalVar: Double, tminSeasonalVar: Double)

object NOAAClustering extends JFXApp {
  //  Future {
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

  val stationsVA = new VectorAssembler().setInputCols(Array("lat", "lon")).setOutputCol("location")
  val stationsWithLoc = stationsVA.transform(stations)
  //  stationsWithLoc.show()

  val kMeans = new KMeans().setK(2000).setFeaturesCol("location").setPredictionCol("cluster")
  val stationClusterModel = kMeans.fit(stationsWithLoc)

  val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
  stationsWithClusters.show()

  //  println(kMeans.explainParams())
  val clu = stationsWithClusters.select('cluster).as[Int].collect()
  val lons = stationsWithClusters.select('lon).as[Double].collect()
  val lats = stationsWithClusters.select('lat).as[Double].collect()

  //{
  //  implicit val df = stationsWithClusters
  // 		val cg = ColorGradient(0.0 -> BlueARGB, 1000.0 -> RedARGB, 2000.0 -> GreenARGB)
  // 		val plot = Plot.scatterPlot(lons, lats, title = "Stations", xLabel = "Longitude", yLabel = "Latitude",
  //		symbolSize = 3, symbolColor = cg(clu))
  //		SwingRenderer(plot, 1000, 650)
  //}

  val data2017 = spark.read.schema(Encoders.product[NOAADataa].schema).
    option("dateFormat", "yyyyMMdd").csv("src/main/scala/Data/2022.csv")

  val joinedData = data2017.filter('measure === "TMAX").join(stationsWithClusters, "sid").cache()

    val withDOYinfo = joinedData.withColumn("doy", dayofyear('date)).
      withColumn("doySin", sin('doy / 365 * 2 * math.Pi)).
      withColumn("doyCos", cos('doy / 365 * 2 * math.Pi))
    val linearRegData = new VectorAssembler().setInputCols(Array("doySin", "doyCos")).
      setOutputCol("doyTrig").transform(withDOYinfo).cache()
    val linearReg = new LinearRegression().setFeaturesCol("doyTrig").setLabelCol("value").
      setMaxIter(10).setPredictionCol("pmaxTemp")
    val linearRegModel = linearReg.fit(linearRegData)
    val withLinearFit = linearRegModel.transform(linearRegData)

  
  
  //Code for plotting the results of the linear regression to fit the sinusoid.
  //y = a*sin(doy) + b*cos(doy) + c
  //    val doy = withLinearFit.select('doy).as[Double].collect(): PlotDoubleSeries
  //    val maxTemp = withLinearFit.select('value).as[Double].collect(): PlotDoubleSeries
  //    val pmaxTemp = withLinearFit.select('pmaxTemp).as[Double].collect(): PlotDoubleSeries
  //    val size1 = 3: PlotDoubleSeries
  //    val size2 = 0: PlotDoubleSeries
  //    val color = BlackARGB: PlotIntSeries
  //    val stroke = renderer.Renderer.StrokeData(1, Nil)
  //    val tempPlot = Plot.scatterPlotsFull(
  //      Array(
  //     (doy, maxTemp, color, size1, , None, None),
  //     (doy, pmaxTemp, color, size2, Some((0: PlotIntSeries) -> stroke), None, None)),
  //      title = "High Temps", xLabel = "Day of Year", yLabel = "Temp")
  //    SwingRenderer(tempPlot, 600, 600)

  spark.stop()
}
