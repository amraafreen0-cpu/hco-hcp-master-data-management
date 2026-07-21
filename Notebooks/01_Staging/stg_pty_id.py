# Databricks notebook source
catalog = "case_study_catalog"
from pyspark.sql.functions import md5, concat_ws, col, trim, upper, regexp_extract, current_timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC # OneKey

# COMMAND ----------


# SQL query to make the canonical model while trimming and uppercasing every column

ok_filtered_df = spark.sql(f"""

SELECT DISTINCT
    TRIM(UPPER(hco_hce_id)) AS src_id,
    "102" AS src_cd,
    "OneKey" AS src_nm,
    TRIM(ORG_DEA) as id_val,
    TRIM(UPPER('DEA')) as id_type,
    'HCO' as entity_type,
    null as attr_1,
    null as attr_2,
    null as pt_data_dt,
    null as mdfd_dt,
    null as crtd_dt
FROM
    {catalog}.raw.one_key_hco
    WHERE COALESCE(TRIM(ORG_DEA), '') != '' and COALESCE(TRIM(hco_hce_id), '') != ''

UNION

SELECT DISTINCT
    TRIM(UPPER(hco_hce_id)) AS src_id,
    "102" AS src_cd,
    "ONEKEY" AS src_nm,
    TRIM(ORG_NPI) as id_val,
    TRIM(UPPER('NPI')) as id_type,
    'HCO' as entity_type,
    null as attr_1,
    null as attr_2,
    null as pt_data_dt,
    null as mdfd_dt,
    null as crtd_dt
FROM
    {catalog}.raw.one_key_hco
    WHERE COALESCE(TRIM(ORG_NPI), '') != '' and COALESCE(TRIM(hco_hce_id), '') != ''

UNION

SELECT 
    DISTINCT TRIM(UPPER(hcp_id)) AS src_id,
    "102" AS src_cd,
    TRIM(UPPER("OneKey")) AS src_nm,
    TRIM(npi) as id_val,
    TRIM(UPPER('npi')) as id_type,
    'HCP' as entity_type,
    null as attr_1,
    null as attr_2,
    null as pt_data_dt,
    null as mdfd_dt,
    null as crtd_dt
FROM
    {catalog}.raw.one_key_hcp  
WHERE
    COALESCE(TRIM(npi), '') != '' and COALESCE(TRIM(UPPER(hcp_id)), '') != ''
""")

ok_filtered_df.createOrReplaceTempView("ok_filtered_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # 867

# COMMAND ----------

# SQL query to make the canonical model while trimming and uppercasing every column

spsd_867_filtered_df = spark.sql(f"""
                           
with raw_867 as (
    select
    md5(concat_ws("|",`Customer Name`, dea, address, city, st, zip)) as src_id, *
    from {catalog}.raw.867_transactions
)

SELECT DISTINCT
    TRIM(UPPER(src_id)) as src_id,
    '105' AS src_cd,
    '867' AS src_nm,
    TRIM(UPPER(DEA)) as id_val,
    TRIM(UPPER('DEA')) as id_type,
    'HCO' as entity_type,
    null as attr_1,
    null as attr_2,
    null as pt_data_dt,
    null as mdfd_dt,
    null as crtd_dt
FROM
    raw_867
    WHERE COALESCE(TRIM(DEA), '') != '' AND TRIM(UPPER(DEA)) != 'XXXXXX' AND TRIM(UPPER(DEA)) != '01'
 

""")

spsd_867_filtered_df.createOrReplaceTempView("spsd_867_filtered_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # Veeva

# COMMAND ----------

# # SQL query to make the canonical model while trimming and uppercasing every column
veeva_filtered_df = spark.sql(f"""
SELECT DISTINCT
    TRIM(id) AS src_id,
    "106" AS src_cd,
    TRIM(UPPER("veeva")) AS src_nm,
    TRIM(UPPER(dea_vod__c)) as id_val,
    TRIM(UPPER('DEA')) as id_type,
    TRIM(UPPER(recordtype)) as entity_type,
    null as attr_1,
    null as attr_2,
    null as pt_data_dt,
    null as mdfd_dt,
    null as crtd_dt
    FROM 
    {catalog}.raw.crm_account  
    WHERE COALESCE(TRIM(dea_vod__c), '') != ''


UNION


SELECT DISTINCT
    TRIM(id) AS src_id,
    "106" AS src_cd,
    TRIM(UPPER("veeva")) AS src_nm,
    TRIM(UPPER(npi_vod__c)) as id_val,
    TRIM(UPPER('NPI')) as id_type,
     TRIM(UPPER(recordtype)) as entity_type,
    null as attr_1,
    null as attr_2,
    null as pt_data_dt,
    null as mdfd_dt,
    null as crtd_dt
    FROM 
    {catalog}.raw.crm_account  
    WHERE  COALESCE(TRIM(npi_vod__c), '') != ''
""")
veeva_filtered_df.createOrReplaceTempView("veeva_filtered_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # Xponent

# COMMAND ----------

# # SQL query to make the canonical model while trimming and uppercasing every column
xponent_filtered_df = spark.sql(f"""
SELECT DISTINCT
    TRIM(iqvia_pres_no) AS src_id,
    "104" AS src_cd,
    TRIM(UPPER("Xponent")) AS src_nm,
    TRIM(UPPER(me_no)) as id_val,
    TRIM(UPPER('ME')) as id_type,
    'HCP' as entity_type,
    null as attr_1,
    null as attr_2,
    null as pt_data_dt,
    null as mdfd_dt,
    null as crtd_dt
    FROM 
    {catalog}.raw.xponent_hcp
    WHERE COALESCE(TRIM(me_no), '') != ''

""")
xponent_filtered_df.createOrReplaceTempView("xponent_filtered_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # Merge

# COMMAND ----------

# Now you can execute your SQL query
merged_pty_identifier_df = spark.sql(f"""
    SELECT *,
        MD5(
            UPPER(
                COALESCE(UPPER(TRIM(attr_1)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(attr_2)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(id_val)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(id_type)), 'NULL') 
            )
        ) AS md5_val
    FROM 
    (
        SELECT src_id, src_cd, src_nm, id_val, id_type, entity_type, attr_1, attr_2, pt_data_dt, mdfd_dt, crtd_dt FROM ok_filtered_view
        UNION 
        SELECT src_id, src_cd, src_nm, id_val, id_type, entity_type,attr_1, attr_2, pt_data_dt, mdfd_dt, crtd_dt FROM spsd_867_filtered_view
        UNION
        SELECT src_id, src_cd, src_nm, id_val, id_type, entity_type,attr_1, attr_2, pt_data_dt, mdfd_dt, crtd_dt FROM veeva_filtered_view
        UNION
        SELECT src_id, src_cd, src_nm, id_val, id_type,entity_type, attr_1, attr_2, pt_data_dt, mdfd_dt, crtd_dt FROM xponent_filtered_view

    )
""")

# Create temporary views for the DataFrames
merged_pty_identifier_df.createOrReplaceTempView("merged_pty_identifier_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # ACD

# COMMAND ----------

# Code to create the new records
added_pty_identifier_df = spark.sql(f"""
SELECT 
    m.src_id, 
    m.src_cd, 
    m.src_nm, 
    m.id_val, 
    m.id_type, 
    m.entity_type,
    m.attr_1, 
    m.attr_2, 
    'A' AS delta_flg, 
    m.md5_val, 
    cast(NULL AS TIMESTAMP) as mdfd_dt, 
    CURRENT_TIMESTAMP() AS crtd_dt, 
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS pt_data_dt
FROM 
    merged_pty_identifier_view m
LEFT ANTI JOIN 
    {catalog}.bronze.stg_pty_id o
ON 
    TRIM(UPPER(concat(m.src_id, m.src_cd, m.id_type))) = TRIM(UPPER(concat(o.src_id, o.src_cd, o.id_type)))
""")
added_pty_identifier_df.createOrReplaceTempView("added_pty_identifier_view")

# Records with updated data 
changed_pty_identifier_df = spark.sql(f"""
SELECT 
    m.src_id, 
    m.src_cd, 
    m.src_nm, 
    m.id_val, 
    m.id_type,
    m.entity_type, 
    m.attr_1, 
    m.attr_2, 
    'C' AS delta_flg, 
    m.md5_val, 
    CURRENT_TIMESTAMP() AS mdfd_dt, 
    o.crtd_dt, 
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS pt_data_dt
FROM 
    merged_pty_identifier_view m
INNER JOIN 
    {catalog}.bronze.stg_pty_id o
ON 
    TRIM(UPPER(concat(m.src_id, m.src_cd, m.id_type))) = TRIM(UPPER(concat(o.src_id, o.src_cd, o.id_type)))
WHERE 
    TRIM(UPPER(coalesce(m.md5_val, ''))) <> TRIM(UPPER(coalesce(o.md5_val, '')))
""")
changed_pty_identifier_df.createOrReplaceTempView("changed_pty_identifier_view")

# Records with no change 
unchanged_df = spark.sql(f"""
select * from {catalog}.bronze.stg_pty_id as o
where not exists(
    select * from changed_pty_identifier_view ch where Trim(Concat(ch.src_id,ch.src_cd,ch.id_type,ch.entity_type))=Trim(Concat(o.src_id,o.src_cd,o.id_type, o.entity_type))
)
""")
unchanged_df.createOrReplaceTempView("unchanged_view")

# Code to merge all the records having delta flag
final_pty_df = spark.sql("""
SELECT 
    src_id, 
    src_cd, 
    src_nm, 
    id_val, 
    id_type, 
    entity_type,
    attr_1, 
    attr_2, 
    delta_flg, 
    md5_val, 
    mdfd_dt, 
    crtd_dt, 
    pt_data_dt 
FROM 
(
    SELECT 
        src_id, 
        src_cd, 
        src_nm, 
        id_val, 
        id_type, 
        entity_type,
        attr_1, 
        attr_2, 
        delta_flg, 
        md5_val, 
        mdfd_dt, 
        crtd_dt, 
        pt_data_dt 
    FROM 
        added_pty_identifier_view

    UNION

    SELECT 
        src_id, 
        src_cd, 
        src_nm, 
        id_val, 
        id_type, 
        entity_type,
        attr_1, 
        attr_2, 
        delta_flg, 
        md5_val, 
        mdfd_dt, 
        crtd_dt, 
        pt_data_dt 
    FROM 
        changed_pty_identifier_view

    UNION
    
    SELECT 
        src_id, 
        src_cd, 
        src_nm, 
        id_val, 
        id_type, 
        entity_type,
        attr_1, 
        attr_2, 
        delta_flg, 
        md5_val, 
        mdfd_dt, 
        crtd_dt, 
        pt_data_dt 
    FROM 
        unchanged_view
)
""")


# COMMAND ----------

#Overwrite the stage table.
final_pty_df.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.stg_pty_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # DQM

# COMMAND ----------

QC_ID = '70'
TBL_NM = 'stg_pty_id'
COL_NM = 'id_val'
ERR_VAL = 'Empty_ID_Value'
CRTLY = '0'

df_1 = spark.sql(f"""
    select src_id, src_cd, src_nm,
    "HCO" as entity_type, 
    '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
    '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
    '{CRTLY}' as crtly, pt_data_dt
    from ( select * from 
            {catalog}.bronze.{TBL_NM} 
            where coalesce(trim(id_val), '') = '')
""")

df_1.write.mode("append").saveAsTable(f"{catalog}.bronze.dq_err_lg")

#-------------------------------------------------------------------------------

QC_ID = '71'
TBL_NM = 'stg_pty_id'
COL_NM = 'id_val'
ERR_VAL = 'Invalid_NPI_Digits'
CRTLY = '0'
ENTITY_TYPE = 'HCO'

df_2 = spark.sql(f"""
   select src_id, src_cd, src_nm,
    "HCO" as entity_type, 
    '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
    '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
    '{CRTLY}' as crtly, pt_data_dt
    from 
    (
        select 
            * 
        from 
        {catalog}.bronze.{TBL_NM} 
        where id_type = 'NPI' and trim(id_val) rlike '[^0-9]'
    )
""")
df_2.write.mode("append").saveAsTable(f"{catalog}.bronze.dq_err_lg")

#-------------------------------------------------------------------------------

QC_ID = '72'
TBL_NM = 'stg_pty_id'
COL_NM = 'id_val'
ERR_VAL = 'Invalid_NPI_Length'
CRTLY = '0'
ENTITY_TYPE = 'HCO'

df_3 = spark.sql(f"""
   select src_id, src_cd, src_nm,
    "HCO" as entity_type, 
    '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
    '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
    '{CRTLY}' as crtly, pt_data_dt
    from 
    (
        select 
            * 
        from 
        {catalog}.bronze.{TBL_NM} 
        where id_type = 'NPI' and length(id_val) <> 10
        
    )
""")
df_3.write.mode("append").saveAsTable(f"{catalog}.bronze.dq_err_lg")


QC_ID = '73'
TBL_NM = 'stg_pty_id'
COL_NM = 'id_val'
ERR_VAL = 'DEA_Val_Zero'
CRTLY = '1'


dq_err_lg_df4 = spark.sql(f"""   select src_id, src_cd, src_nm,
    "HCO" as entity_type, 
    '{QC_ID}' as qc_id, '{TBL_NM}' as tbl_nm,
    '{COL_NM}' as col_nm, '{ERR_VAL}' as err_val, 
    '{CRTLY}' as crtly, pt_data_dt
    from ( select * from 
            {catalog}.bronze.{TBL_NM} 
            where id_val = '0' and id_type = 'DEA')
""")


dq_err_lg_df4.write.mode("append").saveAsTable(f"{catalog}.bronze.dq_err_lg")
