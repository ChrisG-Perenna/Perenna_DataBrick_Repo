# Databricks notebook source
# Connect to Blob
v_scope             = "DataBrickScope"

# Values from App Registration
v_ServiceCredential = dbutils.secrets.get(v_scope, key="DataBrickSecret2")
v_ClientId          = dbutils.secrets.get(v_scope, key="DataBrick-ClientId")
v_DirectoryId       = dbutils.secrets.get(v_scope, key="Databrick-DirectoryId")
v_EndPoint          = "https://login.microsoftonline.com/" + v_DirectoryId + "/oauth2/token"
#Storage Account Credentials
v_storageAccount    = "datateamdatalakestorage"
v_ContainerName     = "epc"
v_directoryPath     = "test1"

v_BlobConfig = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id":       v_ClientId,
                "fs.azure.account.oauth2.client.secret":   v_ServiceCredential,
                "fs.azure.account.oauth2.client.endpoint": v_EndPoint}

dbutils.fs.unmount("/mnt/adls2/epc")

# Mount Container (referencing the connection config)
#dbutils.fs.mount(source = "abfss://" + v_ContainerName + "@" + v_storageAccount + ".dfs.core.windows.net/" + v_directoryPath , mount_point = "/mnt/adls2/" +v_ContainerName, extra_configs = v_BlobConfig) 
dbutils.fs.mount(source = "abfss://" + v_ContainerName + "@" + v_storageAccount + ".dfs.core.windows.net"                    , mount_point = "/mnt/adls2/" +v_ContainerName, extra_configs = v_BlobConfig) 



# List Current Mount Points
for mount in dbutils.fs.mounts():  print (mount.mountPoint)



# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP SCHEMA apprivorep_delta CASCADE;
# MAGIC --DROP SCHEMA apprivorep_merge CASCADE;
# MAGIC --DROP SCHEMA apprivorepmerge_delta CASCADE;
# MAGIC --DROP SCHEMA christest_delta CASCADE;
# MAGIC --DROP SCHEMA d3e_delta CASCADE;
# MAGIC --DROP SCHEMA qlm_delta CASCADE;
# MAGIC --DROP SCHEMA wps_delta CASCADE;
# MAGIC 
# MAGIC DROP SCHEMA zstg_apprivorep CASCADE;
# MAGIC DROP SCHEMA apprivorep CASCADE;
# MAGIC CREATE SCHEMA zstg_apprivorep;
# MAGIC CREATE SCHEMA apprivorep;
# MAGIC 
# MAGIC 
# MAGIC --CREATE SCHEMA zstg_apprivo;
# MAGIC --CREATE SCHEMA zstg_apprivorep;
# MAGIC --CREATE SCHEMA zstg_qlm;
# MAGIC --CREATE SCHEMA zstg_d3e;
# MAGIC --CREATE SCHEMA zstg_wps;

# COMMAND ----------

#dbutils.fs.ls("/mnt/adls2/delta-write/adf-copy/RptAccount")
#List files using Spark API Format
dbutils.fs.ls("dbfs:/user/hive/warehouse/finova.db/rptbrokeruser/_delta_log")
dbutils.fs.ls("/mnt/adls2/delta-write")

# COMMAND ----------


