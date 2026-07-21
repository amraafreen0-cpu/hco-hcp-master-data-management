# Databricks notebook source
catalog = "case_study_catalog"
from pyspark.sql.functions import md5
, concat_ws, col, trim, upper, regexp_extract, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC # **DDD**

# COMMAND ----------

ddd_filtered_df = spark.sql(f"""
                            
with ddd_data as (
select * from {catalog}.raw.ddd_hco
)
SELECT
    DISTINCT TRIM(UPPER(A.OUTLET_ID)) AS src_id,
    "103" AS src_cd,
    "DDD" AS src_nm,
    TRIM(UPPER(A.OUTLET_NAME)) AS full_nm,
    NULL AS acnt_cd1,
    NULL AS acnt_cd2,
    NULL AS acnt_cd3,
    NULL AS acnt_cd4,
    NULL AS acnt_desc1,
    NULL AS acnt_desc2,
    NULL AS acnt_desc3,
    NULL AS acnt_desc4,
    "A" AS pty_status,
    NULL AS is_delete,
    NULL AS pty_strsn_cd,
    NULL AS kaiser_flg,
    "1" AS attr_1,
    NULL AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM
    ddd_data A
WHERE COALESCE(TRIM(UPPER(A.OUTLET_NAME)),'' )!= ''

UNION

SELECT 
    DISTINCT md5(concat_ws("|",facility_name, facility_address1, facility_state, facility_zip)) AS src_id,
    "103" AS src_cd,
    "DDD" AS src_nm,
    TRIM(UPPER(FACILITY_NAME)) AS full_nm,
    NULL AS acnt_cd1,
    NULL AS acnt_cd2,
    NULL AS acnt_cd3,
    NULL AS acnt_cd4,
    NULL AS acnt_desc1,
    NULL AS acnt_desc2,
    NULL AS acnt_desc3,
    NULL AS acnt_desc4,
    "A" AS pty_status,
    NULL AS is_delete,
    NULL AS pty_strsn_cd,
    NULL AS kaiser_flg,
    "5" AS attr_1,
    NULL AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM 
    ddd_data 
WHERE COALESCE(TRIM(UPPER(FACILITY_NAME)),'' )!= ''
"""
)

ddd_filtered_df.createOrReplaceTempView("ddd_filtered_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # **VEEVA**

# COMMAND ----------

veeva_filtered_df = spark.sql(
    f"""
    SELECT DISTINCT
        TRIM(veeva_account_raw.id) AS src_id,
        "106" AS src_cd,
        TRIM(UPPER('veeva')) AS src_nm,
        TRIM(UPPER(veeva_account_raw.name)) AS full_nm,
        NULL AS acnt_cd1,
        NULL AS acnt_cd2,
        TRIM(UPPER(veeva_account_raw.specialty_1_vod__c)) AS acnt_cd3,
        NULL AS acnt_cd4,
        NULL AS acnt_desc1,
        TRIM(UPPER(NPI_vod__c)) AS acnt_desc2,
        TRIM(UPPER(veeva_account_raw.specialty_1_vod__c)) AS acnt_desc3,
        NULL AS acnt_desc4,
        "A" AS pty_status,
        TRIM(UPPER(Account_Identifier_vod__c)) AS is_delete,
        NULL AS pty_strsn_cd,
        NULL AS kaiser_flg,
        "5" AS attr_1,
        NULL AS attr_2,
        NULL AS delta_flg,
        NULL AS mdfd_dt,
        NULL AS crtd_dt,
        NULL AS pt_data_dt
    FROM 
        {catalog}.raw.crm_account veeva_account_raw
    WHERE 
        UPPER(recordtype) = 'HCO'
    """
)

veeva_filtered_df.createOrReplaceTempView("veeva_filtered_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # OneKey

# COMMAND ----------

ok_filtered_df = spark.sql(f"""
SELECT
    DISTINCT TRIM(UPPER(hco_hce_id)) AS src_id,
    "102" AS src_cd,
    "ONEKEY" AS src_nm,
    bus_nm AS full_nm,
    NULL AS acnt_cd1,
    NULL AS acnt_cd2,
    NULL AS acnt_cd3,
    NULL AS acnt_cd4,
    NULL AS acnt_desc1,
    NULL AS acnt_desc2,
    NULL AS acnt_desc3,
    NULL AS acnt_desc4,
    "A" AS pty_status,
    NULL AS is_delete,
    NULL AS pty_strsn_cd,
    NULL AS kaiser_flg,
    "1" AS attr_1,
    NULL AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM
    {catalog}.raw.one_key_hco
WHERE
    COALESCE(TRIM(UPPER(hco_hce_id)),'' )!= ''

"""
)

ok_filtered_df.createOrReplaceTempView("ok_filtered_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # 867

# COMMAND ----------

spsd_867_filtered_df = spark.sql(f"""
                            
with raw_867 as (
    select
    md5(concat_ws("|",`Customer Name`, dea, address, city, st, zip)) as src_id1,
    md5(concat_ws("|",`Wholesaler`, dea, address, city, st, zip)) as src_id2,
     *
    from {catalog}.raw.867_transactions
)


-- SELECT
--     DISTINCT
--     TRIM(UPPER(src_id2)) as src_id,
--     TRIM('105') AS src_cd,
--     TRIM(UPPER('867')) AS src_nm,
--     TRIM(UPPER(wholesaler)) AS full_nm,
--     'WHOLESALER' AS acnt_cd1,
--     NULL AS acnt_cd2,
--     NULL AS acnt_cd3,
--     NULL AS acnt_cd4,
--     'WHOLESALER' AS acnt_desc1,
--     NULL AS acnt_desc2,
--     NULL AS acnt_desc3,
--     NULL AS acnt_desc4,
--     "A" AS pty_status,
--     NULL AS isdelete,
--     NULL AS pty_strsn_cd,
--     NULL AS kaiser_flg,
--     "1" AS attr_1,
--     NULL AS attr_2,
--     NULL AS delta_flg,
--     NULL AS mdfd_dt,
--     NULL AS crtd_dt,
--     NULL AS pt_data_dt
-- FROM
--     raw_867
-- GROUP BY ALL

-- UNION 

SELECT
    DISTINCT 
    TRIM(UPPER(src_id1)) as src_id,
    TRIM('105') AS src_cd,
    TRIM(UPPER('867')) AS src_nm,
    TRIM(UPPER(`Customer Name`)) AS full_nm,
    'CUSTOMER' AS acnt_cd1,
    NULL AS acnt_cd2,
    NULL AS acnt_cd3,
    NULL AS acnt_cd4,
    'CUSTOMER' AS acnt_desc1,
    NULL AS acnt_desc2,
    NULL AS acnt_desc3,
    NULL AS acnt_desc4,
    "A" AS pty_status,
    NULL AS isdelete,
    NULL AS pty_strsn_cd,
    NULL AS kaiser_flg,
    "1" AS attr_1,
    NULL AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM
    raw_867
GROUP BY ALL
"""
)

spsd_867_filtered_df.createOrReplaceTempView("spsd_867_filtered_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # Merged

# COMMAND ----------

# SQL Query to calculate the MD5 hash of the columns while merging the temporary views
merged_pty_df = spark.sql(f"""
    SELECT *,
        MD5(
            UPPER(
                COALESCE(UPPER(TRIM(full_nm)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_cd1)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_cd2)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_cd3)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_cd4)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_desc1)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_desc2)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_desc3)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(acnt_desc4)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(pty_status)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(pty_strsn_cd)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(kaiser_flg)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(attr_1)), 'NULL') || '|' ||
                 COALESCE(UPPER(TRIM(src_id)), 'NULL') || '|' ||
                  COALESCE(UPPER(TRIM(src_cd)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(attr_2)), 'NULL')
            )
        ) AS md5_val
    FROM 
    (
        SELECT * FROM ddd_filtered_view
        UNION
        SELECT * FROM ok_filtered_view
        UNION
        SELECT * FROM veeva_filtered_view
        UNION
        SELECT * FROM spsd_867_filtered_view
    )
""")


# Create temporary views for the DataFrames
merged_pty_df.createOrReplaceTempView("merged_pty_view")
# merged_pty_df.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.stg_pty_hco")

# Write final data to target table using the DataFrame directly
# merged_pty_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.bronze.stg_pty_hco")

# COMMAND ----------

# MAGIC %md
# MAGIC # ACD

# COMMAND ----------


# Code to create the Added records
added_pty_df = spark.sql(f"""
    SELECT m.src_id, m.src_cd, m.src_nm, m.full_nm,
    m.acnt_cd1, m.acnt_cd2, m.acnt_cd3, m.acnt_cd4,
    m.acnt_desc1, m.acnt_desc2, m.acnt_desc3, m.acnt_desc4,
    m.pty_status, m.pty_strsn_cd, m.kaiser_flg, 
    m.attr_1, m.attr_2, m.md5_val, 
    'A' AS delta_flg,
    null AS mdfd_dt,
    CURRENT_TIMESTAMP() AS crtd_dt,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS pt_data_dt
    FROM merged_pty_view m
    LEFT ANTI JOIN
        {catalog}.bronze.stg_pty_hco o
    ON
        TRIM(CONCAT(m.src_id, m.src_cd)) = TRIM(CONCAT(o.src_id, o.src_cd))
""")

added_pty_df.createOrReplaceTempView("added_pty_view")

# COMMAND ----------

# Code to create the Changed records
changed_pty_df = spark.sql(f"""
    SELECT DISTINCT
        m.src_id, m.src_cd, m.src_nm, m.full_nm,
        m.acnt_cd1, m.acnt_cd2, m.acnt_cd3, m.acnt_cd4,
        m.acnt_desc1, m.acnt_desc2, m.acnt_desc3, m.acnt_desc4,
        m.pty_status, m.pty_strsn_cd, m.kaiser_flg, 
        m.attr_1, m.attr_2, m.md5_val, 
        'C' AS delta_flg,
        CURRENT_TIMESTAMP() AS mdfd_dt,
        o.crtd_dt,
        DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS pt_data_dt
    FROM merged_pty_view m
    INNER JOIN {catalog}.bronze.stg_pty_hco o
    ON TRIM(CONCAT(m.src_id, m.src_cd)) = TRIM(CONCAT(o.src_id, o.src_cd))
    AND TRIM(COALESCE(m.md5_val, '')) <> TRIM(COALESCE(o.md5_val, ''))
""")

changed_pty_df.createOrReplaceTempView("changed_pty_view")

# COMMAND ----------

# Code to create the Unchanged records
unchange_pty_df = spark.sql(f"""
    SELECT * FROM {catalog}.bronze.stg_pty_hco o
    WHERE NOT EXISTS (
        SELECT *
        FROM changed_pty_view ch where trim(concat(ch.src_id, ch.src_cd)) = trim(concat(o.src_id, o.src_cd)))
""")


unchange_pty_df.createOrReplaceTempView("unchange_pty_view")


# COMMAND ----------

# Code to create final dataframe by merging the added, changed and unchanged records
final_pty_df = spark.sql("""
    SELECT DISTINCT
        src_id, src_cd, src_nm, full_nm, 
        acnt_cd1, acnt_cd2, acnt_cd3, acnt_cd4,
        acnt_desc1, acnt_desc2, acnt_desc3, acnt_desc4,
        pty_status, pty_strsn_cd, kaiser_flg, attr_1, attr_2, 
        md5_val, delta_flg, mdfd_dt, crtd_dt, pt_data_dt 
    FROM (
        SELECT * FROM added_pty_view
        UNION
        SELECT * FROM changed_pty_view
        UNION
        SELECT * FROM unchange_pty_view
    )
""")

final_pty_df.createOrReplaceTempView("final_pty_view")

# COMMAND ----------

final_pty_df.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.stg_pty_hco")

# COMMAND ----------

# MAGIC %md
# MAGIC ERROR LOGS

# COMMAND ----------

# Code to create the QC Error Log

QC_ID = '50'
TBL_NM = 'stg_pty_hco'
COL_NM = 'full_nm'
ERR_VAL = 'Empty_full_nm'
CRTLY = '1'

dq_err_lg_df1 = spark.sql(f"""
    select src_id, src_cd, src_nm,
    "HCO" as entity_type, 
    '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
    '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
    '{CRTLY}' as crtly, pt_data_dt
    from ( select * from 
            {catalog}.bronze.{TBL_NM} 
            where coalesce(trim(full_nm), '') = '')
""")


dq_err_lg_df1.write.mode("append").saveAsTable(f"{catalog}.bronze.dq_err_lg")


#-------------------------------------------------------------------------------


# QC_ID = '51'
# TBL_NM = 'stg_pty_hco'
# COL_NM = 'acnt_cd1'
# ERR_VAL = 'Empty_acnt_cd1'
# CRTLY = '0'

# dq_err_lg_df2 = spark.sql(f"""
#     select src_id, src_cd, src_nm,
#     "HCO" as entity_type, 
#     '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
#     '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
#     '{CRTLY}' as crtly, pt_data_dt
#     from ( select * from 
#             {catalog}.bronze.{TBL_NM} 
#             where coalesce(trim(acnt_cd1), '') = '')
# """)


# dq_err_lg_df2.write.mode("append").saveAsTable(f"{catalog}.bronze.dq_err_lg")


#-------------------------------------------------------------------------------


QC_ID = '52'
TBL_NM = 'stg_pty_hco'
COL_NM = 'acnt_cd1'
ERR_VAL = 'Non_cs_Products_867'
CRTLY = '1'

dq_err_lg_df3 = spark.sql(f"""
    select src_id, src_cd, src_nm,
    "HCO" as entity_type, 
    '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
    '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
    '{CRTLY}' as crtly, pt_data_dt
    from ( select * from 
            {catalog}.bronze.{TBL_NM} 
            where src_nm = '867' AND attr_2 = 'N' )
""")


dq_err_lg_df3.write.mode("append").saveAsTable(f"{catalog}.bronze.dq_err_lg")



#-------------------------------------------------------------------------------
# discuss on cd2

# QC_ID = '32'
# TBL_NM = 'stg_pty_hco'
# COL_NM = 'acnt_cd2'
# ERR_VAL = 'Empty_acnt_cd2'
# CRTLY = '0'

# dq_err_lg_df3 = spark.sql(f"""
#     select src_id, src_cd, src_nm,
#     "HCO" as entity_type, 
#     '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
#     '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
#     '{CRTLY}' as crtly, pt_data_dt
#     from ( select * from 
#             mdm_metastore.comm_mdm_stage.{TBL_NM} 
#             where coalesce(trim(acnt_cd2), '') = '')
# """)


# dq_err_lg_df3.write.mode("append").saveAsTable("mdm_metastore.comm_mdm_stage.dq_err_lg")
