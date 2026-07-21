# Databricks notebook source
catalog = 'case_study_catalog'

from pyspark.sql import Row
import requests
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# COMMAND ----------

# Code to bring the delta flag for delta records
address_hco_with_dqm = spark.sql(f"""
    SELECT A.*,
    CASE 
      WHEN B.crtly = '1'
        THEN 'C'
      WHEN B.crtly = '0'
        THEN 'NC'
      ELSE
        null
    END AS dqm_flg
    FROM 
        {catalog}.bronze.stg_pty_address A
    LEFT JOIN
        (SELECT ROW_NUMBER() over (partition by concat(src_id, src_cd) order by crtly desc, pt_data_dt desc) as rn, * from {catalog}.bronze.dq_err_lg where  qc_id in (80, 81, 82, 83, 84) and entity_type = 'HCO') B
    ON
        upper(trim(concat(A.src_id, A.src_cd))) = upper(trim(concat(B.src_id, B.src_cd)))
        AND B.rn = 1

""")



address_hco_with_dqm.createOrReplaceTempView("address_hco_with_dqm_view")

address_hco_with_eid = spark.sql(f"""
    SELECT B.cs_id  AS cs_id, A.*
    FROM 
        address_hco_with_dqm_view A
    LEFT JOIN
        {catalog}.silver.hco_id_store B
    ON
        upper(trim(concat(A.src_id, A.src_cd))) = upper(trim(concat(B.src_id, B.src_cd))) and A.entity_type = 'HCO' where A.entity_type = 'HCO'
""")

address_hco_with_eid.createOrReplaceTempView("address_hco_with_eid_view")


# COMMAND ----------

# Code to bring the delta flag for delta records
address_hcp_with_dqm = spark.sql(f"""
    SELECT A.*,
    CASE
      WHEN B.crtly = '1'
        THEN 'C'
      WHEN B.crtly = '0'
        THEN 'NC'
      ELSE
        null
    END AS dqm_flg
    FROM 
        {catalog}.bronze.stg_pty_address A
    LEFT JOIN
        (SELECT ROW_NUMBER() over (partition by concat(src_id, src_cd) order by crtly desc, pt_data_dt desc) as rn, * FROM {catalog}.bronze.dq_err_lg where qc_id in (30, 31, 32, 33, 34) AND entity_type = "HCP") B
    ON
      upper(trim(concat(A.src_id, A.src_cd))) = upper(trim(concat(B.src_id, B.src_cd)))
    AND 
      B.rn = 1 
    AND 
      B.entity_type = "HCP"
""")


# address_hcp_with_dqm.display()

address_hcp_with_dqm.createOrReplaceTempView("address_hcp_with_dqm_view")

address_hcp_with_eid = spark.sql(f"""
    SELECT B.cs_id  AS cs_id, A.*
    FROM 
        address_hcp_with_dqm_view A
    LEFT JOIN
        {catalog}.silver.hcp_id_store B
    ON
        UPPER(TRIM(concat(A.src_id, A.src_cd))) = UPPER(TRIM(concat(B.src_id, B.src_cd))) where A.entity_type = 'HCP'
""")
# address_hcp_with_eid.display()

address_hcp_with_eid.createOrReplaceTempView("address_hcp_with_eid_view")


# COMMAND ----------

address_with_eid = spark.sql(f"""
    SELECT * FROM address_hco_with_eid_view
    UNION ALL
    SELECT * FROM address_hcp_with_eid_view
""")
address_with_eid.createOrReplaceTempView("address_with_eid_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # Standardization

# COMMAND ----------


# Code to standardize the delta records using the mapping table
stan_address_df = spark.sql(f"""
    SELECT
    A.cs_id, A.src_id, A.src_cd, A.src_nm, entity_type,
    addr_id, addr_line1, addr_line2,
    city, state, country, LPAD(zip4,4,'0') AS zip4, zip5,
     addr_type, prim_addr_flg,
    address_status,
    coalesce(B.tgt_val, NULL) AS stan_address_status,
    address_verification_status,
    attr_1, attr_2,
    md5_val, delta_flg, mdfd_dt, crtd_dt, pt_data_dt, dqm_flg
    FROM 
      address_with_eid_view A
    LEFT JOIN 
     {catalog}.bronze.stand_map_master B
    ON 
      trim(A.address_status) = trim(B.src_val)
    AND 
       trim(B.stan_type) = 'address_status'
      
""")

# stan_hcp_address_df.display()

stan_address_df.createOrReplaceTempView("stan_address_view")

# COMMAND ----------

# addresses_df = spark.sql(f"""
#     SELECT distinct  concat_ws(',',trim(a.addr_line1),trim(a.addr_line2)) as addr_line1,trim(a.city) city, trim(a.state) state, trim(a.zip5) zip5,trim(a.country) country
#     FROM stan_address_view a
#     WHERE coalesce(a.prim_addr_flg,'N') = 'Y' 
    
# """)
# addresses_df.createOrReplaceTempView("addresses_view")

# new_address = spark.sql(f""" 
#     SELECT * FROM addresses_view   
# """)

# COMMAND ----------

change_pty_address_df = spark.sql(f"""
    SELECT DISTINCT
    A.cs_id,
    A.src_id,
    A.src_cd,
    A.src_nm,
    A.entity_type,
    A.addr_id,
    A.addr_line1,
    A.addr_line2,
    A.city,
    A.state,
    A.country,
    A.zip4,
    A.zip5,
    A.addr_line1 stan_addr_line1,
     A.city as stan_city,
    A.state as stan_state,
    A.ZIP5 as stan_zip5,
    'US' as stan_country,
    A.addr_type,
    A.prim_addr_flg,
    A.address_status,
    A.stan_address_status,
    A.address_verification_status,
    A.attr_1,
    A.attr_2,
    A.delta_flg,
    A.md5_val,
    A.mdfd_dt,
    A.crtd_dt,
    A.pt_data_dt,
    A.dqm_flg
    FROM stan_address_view A
""")
change_pty_address_df.createOrReplaceTempView("change_pty_address_view")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re
 
def standardize_address(input_df, reference_table_path, input_col="stan_addr_line1"):
   
    # Step 1: Load and clean mapping reference
    ref_df = spark.sql(
        f"SELECT unstand_address, stand_address FROM {reference_table_path}"
    )
    
    cleaned_ref = ref_df.select(
        F.lower(F.regexp_replace(F.col("unstand_address"), r'[^a-zA-Z0-9\s]', '')).alias("unstand"),
        F.upper(F.regexp_replace(F.col("stand_address"), r'[^a-zA-Z0-9\s]', '')).alias("stand")
    )
 
    # Step 2: Collect mapping as normal dictionary (no broadcast)
    mappings = {row["unstand"].strip(): row["stand"].strip() for row in cleaned_ref.collect()}
 
    # Step 3: Define UDF logic (dictionary lookup only)
    def replace_standalone_word(full_name):
        if full_name is None:
            return None
 
        # Clean input string
        
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', ' ', full_name.lower())
        words = cleaned.split()
        
        # Replace words based on mappings dict
        result = []
        for w in words:
            if w in mappings:
                result.append(mappings[w])   # mapped uppercase word
            else:
                result.append(w.upper())    # keep original as uppercase
 
        # Build final standardized string
        standardized = " ".join(result).strip()
        return re.sub(r"\s+", " ", standardized)  # normalize spaces
 
    replace_udf = F.udf(replace_standalone_word, StringType())
 
    # Step 4: Apply UDF to input_df
    out_df = input_df.withColumn("stan_addr_line1", replace_udf(F.col(input_col)))
 
    return out_df
 

# COMMAND ----------

df_final = standardize_address(change_pty_address_df, f"{catalog}.bronze.address_standardization_map", input_col="stan_addr_line1")
df_final.createOrReplaceTempView("final_address_view")

# COMMAND ----------


# Code to create final dataframe by merging the delta records (with dqm flag and celltrion) and unchanged records

address_hcp_df = spark.sql("""
    SELECT  DISTINCT 
        cs_id, src_id, src_cd, src_nm, entity_type, addr_id, addr_line1,addr_line2, city, state, zip5,zip4,country, 
        CASE 
            WHEN stan_city LIKE '%,%' 
              OR LENGTH(stan_state) > 2 
              OR COALESCE(stan_addr_line1, '') = '' 
              OR TRIM(stan_addr_line1) = 'N/A N/A' 
            THEN addr_line1 
            ELSE stan_addr_line1 
        END AS stan_addr_line1,
        
        CASE 
            WHEN stan_city LIKE '%,%' 
              OR LENGTH(stan_state) > 2 
              OR COALESCE(stan_addr_line1, '') = '' 
              OR TRIM(stan_addr_line1) = 'N/A N/A' 
            THEN city 
            ELSE stan_city 
        END AS stan_city,

        CASE 
            WHEN stan_city LIKE '%,%' 
              OR LENGTH(stan_state) > 2 
              OR COALESCE(stan_addr_line1, '') = '' 
              OR TRIM(stan_addr_line1) = 'N/A N/A' 
            THEN state 
            ELSE stan_state 
        END AS stan_state,

        CASE 
            WHEN stan_city LIKE '%,%' 
              OR LENGTH(stan_state) > 2 
              OR COALESCE(stan_addr_line1, '') = '' 
              OR TRIM(stan_addr_line1) = 'N/A N/A' 
              OR COALESCE(stan_zip5, '') = '' 
            THEN split(zip5, '-')[0] 
            ELSE stan_zip5 
        END AS stan_zip5,

         stan_country,  addr_type, prim_addr_flg, 
        address_status, stan_address_status, address_verification_status, 
        attr_1, attr_2, delta_flg, md5_val, mdfd_dt, crtd_dt, pt_data_dt, dqm_flg

    FROM --change_pty_address_view
    final_address_view

""")

address_hcp_df.createOrReplaceTempView("address_hcp_view")

# COMMAND ----------

#Overwrite the address premdm table
address_hcp_df.write.mode("overwrite").saveAsTable(f"{catalog}.silver.premdm_pty_address")