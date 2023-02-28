# Databricks notebook source
# Importing packages
import pyspark
import functools
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import substring, length, col, expr, split, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, DecimalType

# Scope for KeyVault credentials
v_scope       = "DataBrickScope"  

# Vars
v_server      = "rp-test-prna.privatelink.database.windows.net"
v_port        = "1433"
v_database    = "Reporting_DW"
##v_sql_orig    = "SELECT DISTINCT REPLACE([Security_PropertyAddressPostCode],' ','')  AS postcode FROM [ApprivoRep].[RptApplication] WHERE [Security_PropertyAddressPostCode] IS NOT NULL AND ac_EPCID IS NULL AND LEN(REPLACE([Security_PropertyAddressPostCode],' ','')) in (5,6,7)"

v_sql_orig    = "SELECT DISTINCT REPLACE([Security_PropertyAddressPostCode],' ','')   AS postcode, ''AS lodgement_datetime, 1 AS Switch FROM [ApprivoRep].[RptApplication] WHERE Security_PropertyAddressPostCode IS NOT NULL AND ac_EPCID IS NULL AND LEN(REPLACE([Security_PropertyAddressPostCode],' ','')) in (5,6,7) UNION SELECT  REPLACE(PropertyPostcode,' ','')  AS postcode, MAX(e.lodgement_date) AS lodgement_datetime, 2 AS Switch FROM [ApprivoRep].[RptApplication] a JOIN [ODS].[PropertyEPCData] e ON e.epcid=a.ac_epcid GROUP BY REPLACE(PropertyPostcode,' ','')"

#v_sql_update  = "SELECT lmk_key, Address, e.lodgement_datetime, e.postcode, building_reference_number, current_energy_rating, potential_energy_rating, current_energy_efficiency, #energy_consumption_current, energy_consumption_potential, co2_emissions_current, co2_emiss_curr_per_floor_area, total_floor_area,uprn, pc.switch FROM epc.epc20221214 e INNER JOIN epc_postcodes #pc ON REPLACE(e.postcode,' ','') = pc.postcode AND pc.postcode IN (SELECT e.postcode FROM ( SELECT REPLACE(postcode,' ','') AS postcode, MAX(lodgement_datetime) AS Deletalodgement_datetime #FROM epc.epc20221214 GROUP BY postcode)e INNER JOIN ( SELECT REPLACE(postcode,' ','') AS Postcode, MAX(lodgement_datetime) AS SQLlodgement_datetime FROM epc_postcodes GROUP BY  #REPLACE(postcode,' ',''))pc ON e.postcode=pc.postcode WHERE Deletalodgement_datetime > SQLlodgement_datetime)"

v_sql_update  = "SELECT lmk_key, Address, e.lodgement_datetime, e.postcode, building_reference_number, current_energy_rating, potential_energy_rating, current_energy_efficiency, energy_consumption_current, energy_consumption_potential, co2_emissions_current, co2_emiss_curr_per_floor_area, total_floor_area,uprn, pc.switch FROM epc.epc20221214 e INNER JOIN epc_postcodes pc ON REPLACE(e.postcode,' ','') = REPLACE(pc.Postcode, ' ', '') AND pc.postcode IN (SELECT e.postcode FROM ( SELECT REPLACE(postcode,' ','') AS postcode, MAX(lodgement_datetime) AS Deletalodgement_datetime FROM epc.epc20221214 GROUP BY postcode)e INNER JOIN ( SELECT REPLACE(postcode,' ','') AS Postcode, MAX(lodgement_datetime) AS SQLlodgement_datetime FROM epc_postcodes GROUP BY  REPLACE(postcode,' ',''))pc ON e.postcode=pc.postcode WHERE Deletalodgement_datetime > SQLlodgement_datetime)"


v_username    = dbutils.secrets.get(v_scope, key = "DB-Username")
v_password    = dbutils.secrets.get(v_scope, key = "DB-Password")

# JDBC Read / Write vars
v_jdbc_url    = f"jdbc:sqlserver://" + v_server + ":" + v_port + ";databaseName=" + v_database 
v_read_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# Read results (Postcodes) from SQL Server into dataframe
Df_sql = (spark.read
  .format("jdbc")\
  .option("driver",                 v_read_driver)\
  .option("url",                    v_jdbc_url)\
  .option("query",                  v_sql_orig)\
  .option("user",                   v_username)\
  .option("password",               v_password)\
  .option("authentication",         "ActiveDirectoryPassword")\
  .option("Encrypt",                "True")\
  .option("TrustServerCertificate", "True")\
  .load()
)

# Put Df_sql results into a Spark SQL temp view
Df_sql.createOrReplaceTempView("epc_postcodes")

# Put Results of v_sql_update query into Dataframe (Joins our epc data to the Spark SQL view we just created)
Df_results = spark.sql(v_sql_update)

# Write dataframe to SQL Server staging table
Df_results.write \
    .format("jdbc")\
    .mode("overwrite")\
    .option("url",                    v_jdbc_url)\
    .option("dbtable",                "zstg_epc.PropertyEPCData")\
    .option("user",                   v_username)\
    .option("password",               v_password)\
    .option("authentication",         "ActiveDirectoryPassword")\
    .option("Encrypt",                "True")\
    .option("TrustServerCertificate", "True")\
    .save()


