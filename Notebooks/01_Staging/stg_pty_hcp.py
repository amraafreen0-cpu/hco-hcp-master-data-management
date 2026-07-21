# Databricks notebook source

##OneKey
from pyspark.sql.functions import md5, concat_ws, col, trim, upper, regexp_extract, current_timestamp
source_catalog = "case_study_catalog.raw"
destination_catalog = "case_study_catalog.bronze"
ok_filtered_df = spark.sql(f"""
SELECT
    DISTINCT TRIM(UPPER(hcp_id)) AS src_id,
    "102" AS src_cd,
    TRIM(UPPER("onekey")) AS src_nm,
    TRIM(UPPER("PRESCRIBER")) AS pty_type,
    CAST(NULL AS STRING) AS gndr_cd,
    TRIM(UPPER(first_name)) AS first_nm,
    CAST(NULL AS STRING) AS middle_nm,
    TRIM(UPPER(last_name)) AS last_nm,
    TRIM(UPPER(CONCAT_WS(' ', TRIM(UPPER(first_name)),TRIM(UPPER(last_name))))) AS full_nm,
    "A" AS pty_status, 
    "ACTIVE" AS pty_strsn_cd, 
    CAST(null AS STRING) AS is_delete,
    CAST(null AS STRING) AS kaiser_flg,
    CAST(null AS STRING) AS delta_flg,
    CAST(null AS STRING) AS attr_1,
    CAST(null AS STRING) AS attr_2
FROM
    {source_catalog}.one_key_hcp
where
    COALESCE(TRIM(UPPER(hcp_id)), '') != ''
"""
)
##Xponent
xpo_filtered_df = spark.sql(f"""
SELECT
    DISTINCT TRIM(UPPER(iqvia_pres_no)) AS src_id,
    "104" AS src_cd,
    TRIM(UPPER("xponent")) AS src_nm,
    TRIM(UPPER("PRESCRIBER")) AS pty_type,
    TRIM(UPPER(REGEXP_EXTRACT(age_gender, '\\s*([MF])$', 1))) AS gndr_cd,
    TRIM(UPPER(pres_fname)) AS first_nm,
    TRIM(UPPER(pres_mid_initial)) AS middle_nm,
    TRIM(UPPER(PRES_LNAME)) AS last_nm,
    TRIM(UPPER(CONCAT_WS(' ', TRIM(UPPER(PRES_FNAME)), TRIM(UPPER(PRES_MID_INITIAL)), TRIM(UPPER(PRES_LNAME))))) AS full_nm, 
    "A" AS pty_status, 
    "ACTIVE" AS pty_strsn_cd,
    CAST(null AS STRING) AS is_delete,
    null AS kaiser_flg,
    null AS delta_flg,
    CAST(null AS STRING) AS attr_1,
    CAST(null AS STRING) AS attr_2
FROM
    {source_catalog}.xponent_hcp
WHERE
    COALESCE(TRIM(UPPER(IQVIA_PRES_NO)), '') != ''
"""
)
#VEEVA
veeva_filtered_df = spark.sql(f"""
SELECT DISTINCT 
    TRIM(id) AS src_id,
    '106' AS src_cd,
    TRIM(UPPER('veeva')) AS src_nm,
    TRIM(UPPER('PRESCRIBER')) AS pty_type,
    CAST(NULL AS STRING) AS gndr_cd,

    TRIM(UPPER(FirstName)) AS first_nm,
    CAST(NULL AS STRING) AS middle_nm,
    TRIM(UPPER(LastName)) AS last_nm,
    TRIM(UPPER(Name)) AS full_nm,

    'A' AS pty_status, 
    'ACTIVE' AS pty_strsn_cd,

    CAST(NULL AS STRING) AS is_delete,
    CAST(NULL AS STRING) AS kaiser_flg,
    CAST(NULL AS STRING) AS delta_flg,

    CAST(null AS STRING)AS attr_1, 

    CAST(NULL AS STRING) AS attr_2

FROM {source_catalog}.crm_account
WHERE TRIM(UPPER(RecordType)) = 'HCP'
""")
# MAGIC - Merging all Data Source's Canonical Model and Generate md5 column
ok_filtered_df.createOrReplaceTempView("ok_filtered_view")
xpo_filtered_df.createOrReplaceTempView("xpo_filtered_view")
veeva_filtered_df.createOrReplaceTempView("veeva_filtered_view")
# SQL Query to calculate the MD5 hash of the columns while merging the temporary views
merged_pty_df = spark.sql(f"""
    SELECT *,
        MD5(
            UPPER(
                COALESCE(UPPER(TRIM(pty_type)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(gndr_cd)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(first_nm)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(middle_nm)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(last_nm)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(full_nm)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(pty_status)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(pty_strsn_cd)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(is_delete)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(kaiser_flg)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(attr_1)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(src_id)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(src_cd)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(attr_2)), 'NULL')
            )
        ) AS md5_val
    FROM 
    (
        SELECT * FROM ok_filtered_view
        UNION
        SELECT * FROM xpo_filtered_view
        UNION
        SELECT * FROM veeva_filtered_view
    )
""")

# Create temporary views for the DataFrames
merged_pty_df.createOrReplaceTempView("merged_pty_view")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create stg_pty_hcp table schema**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS case_study_catalog.bronze.stg_pty_hcp (
# MAGIC     src_id STRING,
# MAGIC     src_cd STRING,
# MAGIC     src_nm STRING,
# MAGIC     pty_type STRING,
# MAGIC     gndr_cd STRING,
# MAGIC     first_nm STRING,
# MAGIC     middle_nm STRING,
# MAGIC     last_nm STRING,
# MAGIC     full_nm STRING,
# MAGIC     pty_status STRING,
# MAGIC     pty_strsn_cd STRING,
# MAGIC     is_delete STRING,
# MAGIC     kaiser_flg STRING,
# MAGIC     attr_1 STRING,
# MAGIC     attr_2 STRING,
# MAGIC     md5_val STRING,
# MAGIC     delta_flg STRING,
# MAGIC     mdfd_dt TIMESTAMP,
# MAGIC     crtd_dt TIMESTAMP,
# MAGIC     pt_data_dt STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC **ACD LOGIC**

# COMMAND ----------

# Check if table has columns before querying
try:
    table_cols = spark.table(f"{destination_catalog}.stg_pty_hcp").columns
    has_columns = len(table_cols) > 0
except:
    has_columns = False

if has_columns:
    added_pty_df = spark.sql(f"""
        SELECT 
            m.src_id, 
            m.src_cd, 
            m.src_nm, 
            m.pty_type, 
            m.gndr_cd, 
            m.first_nm, 
            m.middle_nm, 
            m.last_nm, 
            m.full_nm,
            m.pty_status,
            m.pty_strsn_cd,
            m.is_delete, 
            m.kaiser_flg, 
            m.attr_1, 
            m.attr_2, 
            m.md5_val, 
            CASE 
                WHEN UPPER(m.is_delete) = 'TRUE' THEN 'D' 
                ELSE 'A' 
            END AS delta_flg,
            cast(null as timestamp) AS mdfd_dt,
            CURRENT_TIMESTAMP() AS crtd_dt,
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS pt_data_dt
        FROM 
            merged_pty_view m
        LEFT ANTI JOIN 
            {destination_catalog}.stg_pty_hcp o
        ON 
            trim(concat(m.src_id, m.src_cd)) = trim(concat(o.src_id, o.src_cd))
    """)
else:
    # Table has no columns, all records are new
    added_pty_df = spark.sql(f"""
        SELECT 
            m.src_id, 
            m.src_cd, 
            m.src_nm, 
            m.pty_type, 
            m.gndr_cd, 
            m.first_nm, 
            m.middle_nm, 
            m.last_nm, 
            m.full_nm,
            m.pty_status,
            m.pty_strsn_cd,
            m.is_delete, 
            m.kaiser_flg, 
            m.attr_1, 
            m.attr_2, 
            m.md5_val, 
            CASE 
                WHEN UPPER(m.is_delete) = 'TRUE' THEN 'D' 
                ELSE 'A' 
            END AS delta_flg,
            cast(null as timestamp) AS mdfd_dt,
            CURRENT_TIMESTAMP() AS crtd_dt,
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS pt_data_dt
        FROM 
            merged_pty_view m
    """)

added_pty_df.createOrReplaceTempView("added_pty_view")
if has_columns:
    changed_pty_df = spark.sql(f"""
        SELECT 
            m.src_id, 
            m.src_cd, 
            m.src_nm, 
            m.pty_type, 
            m.gndr_cd, 
            m.first_nm, 
            m.middle_nm, 
            m.last_nm, 
            m.full_nm,
            m.pty_status,
            m.pty_strsn_cd,
            m.is_delete, 
            m.kaiser_flg, 
            m.attr_1, 
            m.attr_2,
            m.md5_val, 
            CASE 
                WHEN UPPER(m.is_delete) = 'TRUE' THEN 'D' 
                ELSE 'C' 
            END AS delta_flg,
            CURRENT_TIMESTAMP() AS mdfd_dt,
            o.crtd_dt,
            DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS pt_data_dt
        FROM 
            merged_pty_view m
        INNER JOIN 
            {destination_catalog}.stg_pty_hcp o
        ON 
            trim(concat(m.src_id, m.src_cd)) = trim(concat(o.src_id, o.src_cd))
            AND trim(coalesce(m.md5_val, '')) <> trim(coalesce(o.md5_val, ''))
    """)
else:
    # Table has no columns, no changed records
    changed_pty_df = spark.createDataFrame([], "src_id STRING, src_cd STRING, src_nm STRING, pty_type STRING, gndr_cd STRING, first_nm STRING, middle_nm STRING, last_nm STRING, full_nm STRING, pty_status STRING, pty_strsn_cd STRING, is_delete STRING, kaiser_flg STRING, attr_1 STRING, attr_2 STRING, md5_val STRING, delta_flg STRING, mdfd_dt TIMESTAMP, crtd_dt TIMESTAMP, pt_data_dt STRING")

changed_pty_df.createOrReplaceTempView("changed_pty_view")

# COMMAND ----------

# Code to create the Unchanged records
if has_columns:
    unchanged_pty_df = spark.sql(f"""
        SELECT 
            o.*
        FROM 
            {destination_catalog}.stg_pty_hcp o
        WHERE 
            NOT EXISTS (
                SELECT *
                FROM 
                    changed_pty_view ch 
                WHERE 
                    trim(concat(ch.src_id, ch.src_cd)) = trim(concat(o.src_id, o.src_cd))
            )
    """)
else:
    # Table has no columns, no unchanged records
    unchanged_pty_df = spark.createDataFrame([], "src_id STRING, src_cd STRING, src_nm STRING, pty_type STRING, gndr_cd STRING, first_nm STRING, middle_nm STRING, last_nm STRING, full_nm STRING, pty_status STRING, pty_strsn_cd STRING, is_delete STRING, kaiser_flg STRING, attr_1 STRING, attr_2 STRING, md5_val STRING, delta_flg STRING, mdfd_dt TIMESTAMP, crtd_dt TIMESTAMP, pt_data_dt STRING")

unchanged_pty_df.createOrReplaceTempView("unchanged_pty_view")

# COMMAND ----------

# MAGIC %md
# MAGIC **MERGING WITH ACD**

# COMMAND ----------


final_pty_df = spark.sql("""
    SELECT src_id, src_cd, src_nm, pty_type,
        gndr_cd, first_nm, middle_nm, last_nm, full_nm,pty_status,
        pty_strsn_cd,is_delete, kaiser_flg, attr_1, attr_2,
        md5_val, delta_flg, mdfd_dt, crtd_dt, pt_data_dt 
        FROM
        (
            SELECT * FROM added_pty_view
            UNION
            SELECT * FROM changed_pty_view
            UNION
            SELECT * FROM unchanged_pty_view
        )
""")
final_pty_df.write.mode("overwrite").saveAsTable(f"{destination_catalog}.stg_pty_hcp")

# COMMAND ----------

# MAGIC %md
# MAGIC **DQM Check**

# COMMAND ----------

QC_ID = '10'
TBL_NM = 'case_study_catalog.bronze.stg_pty_hcp'
COL_NM = 'full_nm'
ERR_VAL = 'Empty_full_nm'
CRTLY = '1'
destination_catalog= "case_study_catalog.bronze"

dq_err_lg_df1 = spark.sql(f"""
    select src_id, src_cd, src_nm,
    "HCP" as entity_type,
    '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
    '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
    '{CRTLY}' as crtly, pt_data_dt 
    from ( select * from case_study_catalog.bronze.stg_pty_hcp where coalesce(trim(full_nm), '') = '')
""")


# COMMAND ----------

dq_err_lg_df1.write.mode("append").saveAsTable(f"{destination_catalog}.dq_err_lg")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TESTING

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.bronze.stg_pty_hcp
