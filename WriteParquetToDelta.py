# Databricks notebook source
###############################################################################
# WRITE PARQUET FILES TO DELTA (STAGING)
###############################################################################

# Import datetime library (to use in the directory path)
import datetime
# Importing packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, length, col, expr, split

# Define Widgets for manual run (Not called by ADF)
#dbutils.widgets.text("p_container",  "apprivorep")
#dbutils.widgets.text("p_write_dest", "to-stage")
#dbutils.widgets.text("p_write_mode", "overwrite")

# Define Variables (based on todays date)
v_sep                        = "/"
v_intial_mount               = 'dbfs:/mnt/adls2'
v_container                  =  dbutils.widgets.get('p_container')  # Receive external param (from ADF)
v_write_dest                 =  dbutils.widgets.get('p_write_dest') # Receive external param (from ADF)
v_write_mode                 = "overwrite" # dbutils.widgets.get('p_write_mode') # Receive external param (from ADF)
v_parquet                    = 'parquet'
v_delta                      = 'delta'
v_land_dir                   = 'land'
v_stage_dir                  = 'zstg'
v_rep_dir                    = 'rep'
v_rep_delta_schema           =  v_container
v_stage_delta_schema         =  v_stage_dir + "_" + v_container
v_land_parq_read_dir_path    = v_intial_mount + v_sep + v_container + v_sep + v_land_dir  + v_sep + datetime.datetime.now().strftime("%Y/%m/%d") 
v_stage_delta_write_dir_path = v_intial_mount + v_sep + v_container + v_sep + v_stage_dir  
v_rep_delta_write_dir_path   = v_intial_mount + v_sep + v_container + v_sep + v_rep_dir  

#print (v_land_parq_read_dir_path)
#print (v_stage_delta_write_dir_path)
#print (v_rep_delta_write_dir_path)

# Get list of files in Landing Parquet folder and put in Saprk Dataframe (to allow use of withColumn for Substring)
df_filelist = spark.createDataFrame(dbutils.fs.ls(v_land_parq_read_dir_path))

# New dataframe with additional TableName column substringed from the name column
df_TableName = df_filelist.withColumn("TableName",expr("substring(name, 1, length(name)-8)").cast("String"))#.show()

#display(df_TableName)
 

# Iterate over the Tablename (to create the data tables)
# https://www.geeksforgeeks.org/how-to-iterate-over-rows-and-columns-in-pyspark-dataframe/
for v_iterator in df_TableName.collect():
    v_land_location  = v_land_parq_read_dir_path    + v_sep + v_iterator["TableName"] + '.' + v_parquet
    v_zstg_location  = v_stage_delta_write_dir_path + v_sep + v_iterator["TableName"]   
    v_rep_location   = v_rep_delta_write_dir_path   + v_sep + v_iterator["TableName"]   
    
    #print(v_iterator["TableName"])
    #print (v_land_location)  
    #print (v_zstg_location)  
    #print (v_rep_location)  
   
    #################################################################################
    ## CREATE NEW DELTA FILE AND TABLE FROM PARQUET FILES IN LANDING DIRECTORY   ####    
    #################################################################################
    # Read a parquet files into a dataframe. Choose whether .mode = overwrite or append
    # This is staging so we overwrite the previous file with the current one
    df_ParqFile = spark.read.parquet(v_land_location)

    
    if (v_write_dest == v_rep_dir):
        #print ("reporting")
        # Write to delta files rep to create the initial rep full tables (OVERWRITE).
        # Then this part of the If is not used again as we have replaced it with MERGE functionality
        df_ParqFile.write.format('delta') \
           .option("overwriteSchema", "false") \
           .option("path", v_rep_location)  \
           .mode(v_write_mode).saveAsTable(v_rep_delta_schema + "." + v_iterator["TableName"])                    
    elif (v_write_dest == v_stage_dir):
        #print("staging")
        # Write delta files to zstg (OVERWRITE), but preserve schema because we have added
        # additional columns to the delta table after the parquet load
        df_ParqFile.write.format('delta') \
           .option("overwriteSchema", "false") \
           .option("path", v_zstg_location)  \
           .mode(v_write_mode).saveAsTable(v_stage_delta_schema + "." + v_iterator["TableName"])     
    
