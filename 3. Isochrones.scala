// Databricks notebook source
// MAGIC %md ## Fake Input Data

// COMMAND ----------

// MAGIC %python 
// MAGIC !pip install lxml geopandas

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC from pyspark.databricks.sql.functions import *
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC lst = []
// MAGIC for ix in range(1,13):
// MAGIC   df_tmp = pd.read_html(f"https://www.latlong.net/category/cities-236-15-{ix}.html")[0]
// MAGIC   lst.append(df_tmp)
// MAGIC df = pd.concat(lst)

// COMMAND ----------

// MAGIC %python
// MAGIC import geopandas as gpd
// MAGIC 
// MAGIC gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude)).to_wkt()
// MAGIC 
// MAGIC (
// MAGIC   spark.createDataFrame(gdf)
// MAGIC   .withColumn("h3", h3_pointash3("geometry", lit(1)))
// MAGIC   .createOrReplaceTempView("sample_cities")
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from sample_cities 

// COMMAND ----------

// MAGIC %md ## Isochrones

// COMMAND ----------

// Imports
import com.graphhopper.GraphHopper
import com.graphhopper.config.Profile
import com.graphhopper.isochrone.algorithm.ShortestPathTree
import com.graphhopper.routing.ev._
import com.graphhopper.routing.querygraph.QueryGraph
import com.graphhopper.routing.util.DefaultSnapFilter
import com.graphhopper.routing.util.EncodingManager
import com.graphhopper.routing.util.TraversalMode
import com.graphhopper.routing.weighting.FastestWeighting
import com.graphhopper.storage.index.Snap
import com.graphhopper.isochrone.algorithm.JTSTriangulator
import com.graphhopper.isochrone.algorithm.ContourBuilder
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier.simplify
import java.util.function.ToDoubleFunction
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import spark.implicits._



// COMMAND ----------

// Roughly one metre in degrees
val oneMetre = (0.00001 - 	0.000001)
// Our tolerance for geographical rounding
val toleranceInMeter = 250 * oneMetre 
// One minute in milliseconds
val oneMinute = 1000 * 60
// Buckets (in minutes) we want to generate our isochrones by. 
val buckets = (20 to 60 by 20).map(_ * oneMinute)
// Our Time Limit not to search beyond
val timeLimit = 70 * 60 * 1000

// COMMAND ----------

// Defining our GraphHopper Instantiator
def create_hopper_instance(osmPath: String): (GraphHopper, EncodingManager, BooleanEncodedValue, DecimalEncodedValue, FastestWeighting) = {
   val hopper = new GraphHopper()
   hopper.setAllowWrites(false) // False so that multiple partitions can run concurrently. 
   hopper.setGraphHopperLocation(osmPath)
   hopper.setProfiles(new Profile("car").setVehicle("car").setWeighting("fastest").setTurnCosts(false))
   hopper.importOrLoad()
  
  val encodingManager = hopper.getEncodingManager()
  val accessEnc = encodingManager.getBooleanEncodedValue(VehicleAccess.key("car"))
  val speedEnc = encodingManager.getDecimalEncodedValue(VehicleSpeed.key("car"))
  val weighting = new FastestWeighting(accessEnc, speedEnc)
  
  return (hopper, encodingManager, accessEnc, speedEnc, weighting)
}

// Defining Limiting Function
def fz: ToDoubleFunction[ShortestPathTree.IsoLabel] = {
        l => l.time
}

// COMMAND ----------

val source = spark
  .read
  .table("sample_cities")

val h3_indices = source
    .select("h3")
    .distinct()
    .collect
    .map(x => x(0).toString)

// COMMAND ----------

h3_indices.foreach { h3 =>
    source
        .filter(s"h3 == $h3")
        .drop("geometry")
        .createOrReplaceTempView(s"table_$h3")
}
case class Record(id: String, latitude: Double, longitude: Double, h3: Long)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from table_582178212767858687

// COMMAND ----------

h3_indices
//.filter(_.startsWith("582178212767858687"))
.foreach { h3 =>
    val df = spark.read.table(s"table_$h3")

    df.mapPartitions( partition => {
    val osmPath = s"/dbfs/datasets/graphhopper/osm/na-split/graphHopperData/$h3/"
    val (hopper, encodingManager, accessEnc, speedEnc, weighting) = create_hopper_instance(osmPath)
    partition.map( row => {
      val id  = row.get(0).asInstanceOf[String]
      val lat = row.get(1).asInstanceOf[Double]
      val lon = row.get(2).asInstanceOf[Double]
      val snap = hopper.getLocationIndex().findClosest(lat, lon, new DefaultSnapFilter(weighting, encodingManager.getBooleanEncodedValue(Subnetwork.key("car"))))
      val queryGraph = QueryGraph.create(hopper.getBaseGraph(), snap)
      val tree = new ShortestPathTree(queryGraph, weighting, false, TraversalMode.NODE_BASED)
      tree.setTimeLimit(timeLimit)
      
      val triangulator = new JTSTriangulator(hopper.getRouterConfig())
      val result = triangulator.triangulate(snap, queryGraph, tree, fz, toleranceInMeter)
      val contourBuilder = new ContourBuilder(result.triangulation)
      
      var isochrones = ListBuffer[(Int, String)]()
      
      for (limit <- buckets) {    
        val isochrone = contourBuilder.computeIsoline(limit, result.seedEdges)
        val simple_isochrone = simplify(isochrone, toleranceInMeter)
        isochrones += ((limit / oneMinute, simple_isochrone.toString))
      }
      (id, isochrones)
    })
  })
  .select(
    col("_1").alias("location_id"),
    explode(col("_2").alias("col")))
  .select("location_id", "col.*")
  .select($"location_id", col("_1").alias("time_min"), col("_2").alias("geom"))
  .createOrReplaceTempView(s"isochrone_$h3")
}

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from isochrone_582178212767858687 
// MAGIC UNION 
// MAGIC SELECT * from isochrone_581707621791170559 

// COMMAND ----------


