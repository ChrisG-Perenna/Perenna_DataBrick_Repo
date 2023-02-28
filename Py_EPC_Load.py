# Databricks notebook source
# Import datetime library (to use in the directory path)
import datetime
# Importing packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, length, col, expr, split, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, DecimalType, TimestampType


# Define Variables (based on todays date)
v_sep                        = "/"
v_intial_mount               = 'dbfs:/mnt/adls2'
v_container                  = 'epc' #dbutils.widgets.get('p_container')  # Receive external param (from ADF)
v_wildcard                   = '*'
v_parquet                    = 'parquet'
v_delta                      = 'delta'
v_land_csv_read_dir_path     = v_intial_mount + v_sep + v_container 
v_write_parquet_dir_path     = v_intial_mount + v_sep + v_container + v_sep + v_parquet + v_sep 
v_write_delta_dir_path       = v_intial_mount + v_sep + v_container + v_sep + v_delta   + v_sep 
v_new_filename               = v_container + datetime.datetime.now().strftime("%Y%m%d")
v_parquet_path_file          = v_write_parquet_dir_path + v_sep + v_new_filename
v_delta_path_file            = v_write_delta_dir_path   + v_sep + v_new_filename

v_schema = StructType() \
      .add("lmk_key", StringType(), True)  \
      .add("Address1", StringType(), True)  \
      .add("Address2", StringType(), True)  \
      .add("Address3", StringType(), True)  \
      .add("postcode", StringType(), True)  \
      .add("building_reference_number", StringType(), True)  \
      .add("current_energy_rating", StringType(), True)  \
      .add("potential_energy_rating", StringType(), True)  \
      .add("current_energy_efficiency", StringType(), True)  \
      .add("potential_energy_efficiency", IntegerType(), True)  \
      .add("property_type", StringType(), True)  \
      .add("built_form", StringType(), True)  \
      .add("inspection_date", DateType(), True)  \
      .add("local_authority", StringType(), True)  \
      .add("constituency", StringType(), True)  \
      .add("county", StringType(), True)  \
      .add("lodgement_date", DateType(), True)  \
      .add("transaction_type", StringType(), True)  \
      .add("environment_impact_current", StringType(), True)  \
      .add("environment_impact_potential", StringType(), True)  \
      .add("energy_consumption_current", FloatType(), True)  \
      .add("energy_consumption_potential", FloatType(), True)  \
      .add("co2_emissions_current", DecimalType(8,2), True) \
      .add("co2_emiss_curr_per_floor_area", DecimalType(8,2), True) \
      .add("co2_emissions_potential", StringType(), True)  \
      .add("lighting_cost_current", FloatType(), True)  \
      .add("lighting_cost_potential", FloatType(), True)  \
      .add("heating_cost_current", FloatType(), True)  \
      .add("heating_cost_potential", FloatType(), True)  \
      .add("hot_water_cost_current", FloatType(), True)  \
      .add("hot_water_cost_potential", FloatType(), True)  \
      .add("total_floor_area", IntegerType(), True) \
      .add("energy_tariff", StringType(), True)  \
      .add("mains_gas_flag", StringType(), True)  \
      .add("floor_level", StringType(), True)  \
      .add("flat_top_storey", StringType(), True)  \
      .add("flat_storey_count", IntegerType(), True)  \
      .add("main_heating_controls", StringType(), True)  \
      .add("multi_glaze_proportion", FloatType(), True)  \
      .add("glazed_type", StringType(), True)  \
      .add("glazed_area", StringType(), True)  \
      .add("extension_count", FloatType(), True)  \
      .add("number_habitable_rooms", FloatType(), True)  \
      .add("number_heated_rooms", FloatType(), True)  \
      .add("low_energy_lighting", IntegerType(), True)  \
      .add("number_open_fireplaces", IntegerType(), True)  \
      .add("hotwater_description", StringType(), True)  \
      .add("hot_water_energy_eff", StringType(), True)  \
      .add("hot_water_env_eff", StringType(), True)  \
      .add("floor_description", StringType(), True)  \
      .add("floor_energy_eff", StringType(), True)  \
      .add("floor_env_eff", StringType(), True)  \
      .add("windows_description", StringType(), True)  \
      .add("windows_energy_eff", StringType(), True)  \
      .add("windows_env_eff", StringType(), True)  \
      .add("walls_description", StringType(), True)  \
      .add("walls_energy_eff", StringType(), True)  \
      .add("walls_env_eff", StringType(), True)  \
      .add("secondheat_description", StringType(), True)  \
      .add("sheating_energy_eff", StringType(), True)  \
      .add("sheating_env_eff", StringType(), True)  \
      .add("roof_description", StringType(), True)  \
      .add("roof_energy_eff", StringType(), True)  \
      .add("roof_env_eff", StringType(), True)  \
      .add("mainheat_description", StringType(), True)  \
      .add("mainheat_energy_eff", StringType(), True)  \
      .add("mainheat_env_eff", StringType(), True)  \
      .add("mainheatcont_description", StringType(), True)  \
      .add("mainheatc_energy_eff", StringType(), True)  \
      .add("mainheatc_env_eff", StringType(), True)  \
      .add("lighting_description", StringType(), True)  \
      .add("lighting_energy_eff", StringType(), True)  \
      .add("lighting_env_eff", StringType(), True)  \
      .add("main_fuel", StringType(), True)  \
      .add("wind_turbine_count", FloatType(), True)  \
      .add("heat_loss_corridor", StringType(), True)  \
      .add("unheated_corridor_length", DecimalType(8,0), True)  \
      .add("floor_height", DecimalType(8,0), True)  \
      .add("photo_supply", FloatType(), True)  \
      .add("solar_water_heating_flag", StringType(), True)  \
      .add("mechanical_ventilation", StringType(), True)  \
      .add("address", StringType(), True)  \
      .add("local_authority_label", StringType(), True)  \
      .add("constituency_label", StringType(), True)  \
      .add("posttown", StringType(), True)  \
      .add("construction_age_band", StringType(), True)  \
      .add("lodgement_datetime", TimestampType(), True)  \
      .add("tenure", StringType(), True)  \
      .add("fixed_lighting_outlets_count", FloatType(), True)  \
      .add("low_energy_fixed_light_count", FloatType(), True)  \
      .add("uprn", StringType(), True) \
      .add("uprn_source", StringType(), True)  


# Get list of folders
df_list = spark.createDataFrame(dbutils.fs.ls(v_land_csv_read_dir_path))

#Setup Columns to get Ful path and Filter string
df_TableName1 = df_list.withColumn("FileName", lit('certificates.csv').cast("String"))
df_TableName2 = df_TableName1.withColumn("FullPath", concat(df_TableName1.path,df_TableName1.FileName))
df_TableName3 = df_TableName2.withColumn("Filterstring",expr("substring(name, 1,8)").cast("String"))

# Filter the Dataframe to relevant paths
df = df_TableName3.filter(df_TableName3.Filterstring == 'domestic')
#display(df)
  
# Loop round reading CSV and appending to delta
for v_iterator in df.collect():   
    
    # Read the CSV into a dataframe
    df_csv_file = spark.read.format("csv") \
        .option("header", True) \
        .schema(v_schema) \
        .load(v_iterator["FullPath"])


    # Create and Append to Delta file and table using the dataframe containing the CSV
    df_csv_file.write.format('delta') \
        .option("header",          "true") \
        .option("path",            v_delta_path_file)  \
        .mode("append").saveAsTable("epc" + "." + v_new_filename)        

    
        #.partitionBy("postcode")      \
        #.option("overwriteSchema", "true") \    

# COMMAND ----------

# MAGIC %sql
# MAGIC --select ROW_NUMBER() OVER(PARTITION BY postcode ORDER BY LODGEMENT_DATE) AS RowNumber , * from epc.epc20221208 where postcode='M16 8FW';
# MAGIC --drop table epc.epc20221208;
# MAGIC --REPLACE TABLE epc.epc20221208 USING DELTA PARTITIONED BY "POSTCODE" AS  SELECT * FROM epc.epc20221208;
# MAGIC SELECT count(0) FROM from epc.epc20221208;
