# Databricks notebook source
# MAGIC %sh sudo apt install osmium-tool -y

# COMMAND ----------

# MAGIC %sh 
# MAGIC #cd /dbfs/datasets/graphhopper/osm/
# MAGIC #wget https://download.geofabrik.de/north-america-latest.osm.pbf

# COMMAND ----------

dbutils.fs.ls("dbfs:/datasets/graphhopper/osm/na-split/")

# COMMAND ----------

# MAGIC %sh ls /Workspace/Repos/timo.roest@databricks.com/osm-split/US-config.json

# COMMAND ----------

# MAGIC %sh osmium extract -v -c /Workspace/Repos/timo.roest@databricks.com/osm-split/US-config.json /dbfs/datasets/graphhopper/osm/north-america-latest.osm.pbf --overwrite

# COMMAND ----------

[(x.name, x.size/1000_000_000) for x in dbutils.fs.ls("dbfs:/datasets/graphhopper/osm/na-split/")]

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT h3_h3tostring(581716417884192767);

# COMMAND ----------



# COMMAND ----------


