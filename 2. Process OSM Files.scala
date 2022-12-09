// Databricks notebook source
val files = dbutils.fs.ls("/datasets/graphhopper/osm/na-split/")
    .filter(x => x.name.contains(".osm.pbf"))
    .map(x => (x.name.replace(".osm.pbf", ""), x.path))

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

files.map(
    h3 => {
        val id = h3._1
        val inputPath = "/" + h3._2.replace(":", "")
        val outputPath = s"target/$id/routing-graph-cache"
        
        val hopper = new GraphHopper()
        hopper.setOSMFile(inputPath)
        hopper.setGraphHopperLocation(outputPath)
        hopper.setProfiles(new Profile("car").setVehicle("car").setWeighting("fastest").setTurnCosts(false))
        hopper.getCHPreparationHandler().setCHProfiles(new CHProfile("car"))
        hopper.importOrLoad()

        dbutils.fs.mv(s"file:/databricks/driver/$outputPath", s"dbfs:/datasets/graphhopper/osm/na-split/graphHopperData/$id/", recurse=true)

        "OK!"
    }
)

// COMMAND ----------


