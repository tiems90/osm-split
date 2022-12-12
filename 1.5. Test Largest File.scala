// Databricks notebook source
val largest_file = "581716417884192767"
val osmFile = s"/dbfs/datasets/graphhopper/osm/na-split/$largest_file.osm.pbf"

println(osmFile)

// COMMAND ----------

import com.graphhopper.GHRequest
import com.graphhopper.GHResponse
import com.graphhopper.GraphHopper
import com.graphhopper.ResponsePath
import com.graphhopper.config.CHProfile
import com.graphhopper.config.LMProfile
import com.graphhopper.config.Profile
import com.graphhopper.routing.weighting.custom.CustomProfile
import com.graphhopper.util._
import com.graphhopper.util.shapes.GHPoint

// COMMAND ----------

val hopper = new GraphHopper()
hopper.setOSMFile(osmFile)
hopper.setGraphHopperLocation("target/routing-graph-cache")
hopper.setProfiles(new Profile("car").setVehicle("car").setWeighting("fastest").setTurnCosts(false))
hopper.getCHPreparationHandler().setCHProfiles(new CHProfile("car"))
hopper.importOrLoad()

// COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/target/routing-graph-cache", "dbfs:/datasets/graphhopper/osm/na-split/graphHopperData/581716417884192767/", recurse=true)

// COMMAND ----------

// MAGIC %md ## Isochrone Test

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

// COMMAND ----------

// Existing storage location
val osmPath = "/dbfs/datasets/graphhopper/osm/na-split/graphHopperData/581716417884192767/"
// Roughly one metre in degrees
val oneMetre = (0.00001 - 	0.000001)
// Our tolerance for geographical rounding
val toleranceInMeter = 10 * oneMetre 
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

case class Location(latitude: Double, longitude: Double, id: Long)

// Create a list of location data for Washington, DC
val dcLocations = List(
  Location(38.90725, -77.03691, 1),
  Location(38.90724, -77.03692, 2),
  Location(38.90723, -77.03693, 3),
  Location(38.90722, -77.03694, 4),
  Location(38.90721, -77.03695, 5)
)

// Convert the list to a Dataframe
val potential_locations = dcLocations.toDF()
display(potential_locations)

// COMMAND ----------

val isochrones = potential_locations
  .repartition(sc.defaultParallelism * 8)
  .mapPartitions( partition => {
    val (hopper, encodingManager, accessEnc, speedEnc, weighting) = create_hopper_instance(osmPath)
    partition.map( row => {
      val id  = row.get(2).asInstanceOf[Long]
      val lat = row.get(0).asInstanceOf[Double]
      val lon = row.get(1).asInstanceOf[Double]
      
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
        val simple_isochrone = simplify(isochrone, 100 * toleranceInMeter)
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

// COMMAND ----------

display(isochrones)

// COMMAND ----------

// MAGIC %fs ls /datasets/graphhopper/osm/na-split/graphHopperData/

// COMMAND ----------


