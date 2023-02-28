# Databricks notebook source
# DBTITLE 1,HashMerge_Finova
######################################################################################################################
# HashMerge_Finova
# Calls PerSchema_HashMerge for each of the Finova schemas
######################################################################################################################

%python
# For Drop-down lists of parameters if called manually in testing
dbutils.widgets.dropdown("p_write_dest", "zstg",    ["zstg", "rep"])

v_write_dest =  dbutils.widgets.get('p_write_dest') # Receive external param (from ADF)


# Call Notebook for each schema with different parameters. Each schema is called to load PArquet files into the delta staging (zstg) area
dbutils.notebook.run("/Users/cgc@perenna.co.uk/WriteParquetToDelta", 480, {'p_container':  'apprivo',    'p_write_dest': 'zstg'} )
dbutils.notebook.run("/Users/cgc@perenna.co.uk/WriteParquetToDelta", 480, {'p_container':  'apprivorep', 'p_write_dest': 'zstg'} )

# Insert Transformation code here :)

# Call Notebook for each schema with different parameters. Each schema is called to merge the delta stageing table into rep schema
dbutils.notebook.run("/Users/cgc@perenna.co.uk/PerSchema_HashMerge", 480, {'p_container':  'apprivo'   } )
dbutils.notebook.run("/Users/cgc@perenna.co.uk/PerSchema_HashMerge", 480, {'p_container':  'apprivorep'} )

