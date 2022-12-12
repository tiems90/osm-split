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

val num_partitions = source
    .select("h3").distinct().count().toInt

// COMMAND ----------

val isochrones = source
  .repartition(num_partitions,$"h3")
  .mapPartitions( partition => {
    partition.map( row => {
      val id  = row.get(0).asInstanceOf[String]
      val lat = row.get(1).asInstanceOf[Double]
      val lon = row.get(2).asInstanceOf[Double]
      (id)
    })
  })

// COMMAND ----------

// MAGIC %md ## WIP

// COMMAND ----------



// COMMAND ----------

isochrones
.write
.format("delta")
.mode("overwrite")
.option("overwriteSchema", "true")
.saveAsTable("timo.isochrones.full_us")

// COMMAND ----------

val isochrones = source
  .repartition(num_partitions,$"h3")

isochrones.rdd.getNumPartitions

// COMMAND ----------

case class Record(id: String, latitude: Double, longitude: Double, h3: Long)

val x = source.mapPartitions( partition => {
    val first = partition
      .toSeq(0)
    val firstRecord = Record(first.get(0).asInstanceOf[String], first.get(1).asInstanceOf[Double], first.get(2).asInstanceOf[Double], first.get(4).asInstanceOf[Long])
    val h3 = firstRecord.h3
    
    val osmPath = s"/dbfs/datasets/graphhopper/osm/na-split/graphHopperData/$h3/"
    val (hopper, encodingManager, accessEnc, speedEnc, weighting) = create_hopper_instance(osmPath)
    
    partition.map( row => {
      val record = Record(row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Double], row.get(2).asInstanceOf[Double], first.get(4).asInstanceOf[Long])

      (record.id, firstRecord.h3)
    })
  })

display(x)

// COMMAND ----------

x.count()

// COMMAND ----------

source.count()

// COMMAND ----------


