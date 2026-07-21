# Databricks notebook source
# MAGIC %md
# MAGIC ## This staging table will combine Addresses for both HCP and HCO Data

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws, col, trim, upper, regexp_extract, current_timestamp
source_catalog = "case_study_catalog.raw"
destination_catalog = "case_study_catalog.bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle ZIP Function

# COMMAND ----------

from pyspark.sql.functions import col, when, split, lpad, trim

def clean_zip(df):
    return df.withColumn(
        "zip4",
        when(
            col("zip5").contains("-"),
            lpad(trim(split(col("zip5"), "-")[1]), 4, "0")
        ).otherwise(col("zip4"))
    ).withColumn(
        "zip5",
        lpad(trim(split(col("zip5"), "-")[0]), 5, "0")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Modelling

# COMMAND ----------


##XponentHCP
xpo_filtered_df = spark.sql(f"""
SELECT
    DISTINCT TRIM(UPPER(iqvia_pres_no)) AS src_id,
    "104" AS src_cd,
    TRIM(UPPER("XPONENT")) AS src_nm,
    'HCP' AS entity_type,
    md5(concat_ws('|', TRIM(UPPER(pres_st_add)), TRIM(UPPER(pres_city)), TRIM(UPPER(pres_state)), TRIM(UPPER(pres_zip)))) AS addr_id, 
    TRIM(UPPER(pres_st_add)) AS addr_line1,
    CAST(null AS STRING) as addr_line2,
    TRIM(UPPER(pres_city)) AS city,
    TRIM(UPPER(pres_state)) AS state,
    TRIM(UPPER('US')) AS country,
    NULL AS zip4,
   TRIM(pres_zip) AS Zip5,
    NULL AS addr_type,
    'Y' AS prim_addr_flg,
    'A' AS address_status,
    NULL AS is_delete,
    NULL AS address_verification_status,  
   CAST(null AS STRING) AS attr_1,
    CAST(null AS STRING) AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM
    {source_catalog}.xponent_hcp
WHERE coalesce(TRIM(concat_ws(' ', pres_st_add, pres_city, pres_state, pres_zip)), '') != ''
""")
xpo_filtered_df = clean_zip(xpo_filtered_df)
#Veeva
veeva_filtered_df = spark.sql(f"""
SELECT DISTINCT
    TRIM(a.Account_vod__c) AS src_id,
    '106' AS src_cd,
    'VEEVA' AS src_nm,
    CASE WHEN TRIM(UPPER(b.RecordType)) = 'HCO' THEN 'HCO' WHEN TRIM(UPPER(b.RecordType)) = 'HCP' THEN 'HCP' END AS entity_type,
    TRIM(a.Id) AS addr_id,
    TRIM(UPPER(a.Address_vod__c)) AS addr_line1,
    CAST(null AS STRING) as addr_line2,
    TRIM(UPPER(a.City_vod__c)) AS city,
    TRIM(UPPER(a.State_vod__c)) AS state,
    'US' AS country,
    NULL AS zip4,
    TRIM(a.Zip_vod__c) AS zip5,
    NULL AS addr_type,
    CASE WHEN UPPER(a.Primary_vod__c) = 'TRUE' THEN 'Y' ELSE 'N' END AS prim_addr_flg,
    'A' AS address_status,
    NULL AS is_delete,
    NULL AS address_verification_status,
    CAST(null AS STRING) AS attr_1,
    CAST(null AS STRING) AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt

FROM {source_catalog}.crm_address a
JOIN {source_catalog}.crm_account b
ON a.Account_vod__c = b.Id

WHERE COALESCE(TRIM(a.Account_vod__c), '') != ''
  AND COALESCE(TRIM(a.Address_vod__c), '') != ''
""")
veeva_filtered_df = clean_zip(veeva_filtered_df)
##DDD_HCO
ddd_filtered_df = spark.sql(f"""
SELECT
    DISTINCT 
    TRIM(UPPER(outlet_id)) AS src_id,
    "103" AS src_cd,
    "DDD" AS src_nm,
    'HCO' AS entity_type,
    md5(concat_ws('|', outlet_id,TRIM(UPPER(outlet_address1)),TRIM(UPPER(outlet_city)), TRIM(UPPER(outlet_state)), TRIM(UPPER(outlet_zip)))) AS addr_id, 
    TRIM(UPPER(outlet_address1)) AS addr_line1,
    CAST(null AS STRING) as addr_line2,
    TRIM(UPPER(outlet_city)) AS city,
    TRIM(UPPER(outlet_state)) AS state,
    TRIM(UPPER('US')) AS country,
    NULL AS zip4,
    TRIM(UPPER(outlet_zip))  AS zip5,
    
    NULL AS addr_type, 
    'Y' AS prim_addr_flg,
    NULL AS is_delete,
    'A' AS address_status,
    CAST(NULL AS STRING) AS address_verification_status,  
     CAST(null AS STRING) AS attr_1,
     CAST(null AS STRING) AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM
     {source_catalog}.ddd_hco
WHERE
    COALESCE(TRIM(outlet_id),'') <> '' AND 
    coalesce(concat_ws(' ', TRIM(UPPER(outlet_address1)), TRIM(UPPER(outlet_city)), TRIM(UPPER(outlet_state)), TRIM(UPPER(outlet_zip))), '') <> ''
UNION 
SELECT 
    DISTINCT md5(concat_ws("|",facility_name, facility_address1, facility_state, facility_zip, "src_id")) AS src_id,
    "103" AS src_cd,
    "DDD" AS src_nm,
    'HCO' as entity_type,
    md5(concat_ws("|",facility_name, facility_address1, facility_state, facility_zip))AS addr_id, 
    TRIM(UPPER(facility_address1)) AS addr_line1,
    CAST(null AS STRING) as addr_line2,
    null AS city,
    TRIM(UPPER(facility_state)) AS state,
    TRIM(UPPER('US')) AS country,
    NULL  AS zip4,
    TRIM(UPPER(facility_zip)) AS zip5,
    
    NULL AS addr_type, 
    'Y' AS prim_addr_flg,
    NULL AS is_delete,
    'A' AS address_status,
    NULL AS address_verification_status,  
    CAST(null AS STRING) AS attr_1,
    CAST(null AS STRING) AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM
   {source_catalog}.ddd_hco
WHERE
    COALESCE(TRIM(md5(concat_ws("|",facility_name, facility_state, facility_zip))),'') <> '' 
    AND coalesce(TRIM(concat_ws(' ', TRIM(UPPER(facility_address1)), TRIM(UPPER(facility_state)), TRIM(UPPER(facility_zip)) )), '') <> ''
"""
)
ddd_filtered_df = clean_zip(ddd_filtered_df)
spsd_867_filtered_df = spark.sql(f"""
WITH spsd_867 AS (
    SELECT 
        DISTINCT
        Wholesaler,
        `Customer Name` AS customer_name,
        DEA,
        Address,
        City,
        ST,
        Zip,
       md5(concat_ws("|",`Customer Name`, dea, address, city, st, zip)) AS md5_customer_val1,
        md5(concat_ws("|",`Wholesaler`, dea, address, city, st, zip)) AS md5_customer_val2
    FROM {source_catalog}.867_transactions
)
SELECT
    DISTINCT 
    TRIM(UPPER(md5_customer_val1)) AS src_id,
    TRIM('105') AS src_cd,
    TRIM(UPPER('867')) AS src_nm,
    'HCO' AS entity_type,
    MD5(UPPER(COALESCE(UPPER(TRIM(src_id)), 'NULL') || '|' || COALESCE(UPPER(TRIM(Address)), 'NULL') || COALESCE(UPPER(TRIM(Customer_Name)), 'NULL'))) AS addr_id,
    TRIM(UPPER(Address)) AS addr_line1,
    CAST(null AS STRING) as addr_line2,
    TRIM(UPPER(City)) AS city,
    TRIM(UPPER(ST)) AS state,
    TRIM(UPPER('US')) AS country, 
    CASE 
        WHEN CHARINDEX('-', Zip) > 0 THEN LPAD(TRIM(RIGHT(Zip, 4)), 4, 0)
        ELSE NULL
    END AS zip4,
    LPAD(TRIM(LEFT(Zip, 5)), 5, 0) AS zip5,
    
    NULL AS addr_type, 
    'Y' AS prim_addr_flg,
    NULL as is_delete,
    'A' AS address_status,  
    NULL AS address_verification_status,
    CAST(null AS STRING) AS attr_1,
    CAST(null AS STRING) AS attr_2,
    NULL AS delta_flg,
    NULL AS mdfd_dt,
    NULL AS crtd_dt,
    NULL AS pt_data_dt
FROM spsd_867
WHERE coalesce(TRIM(concat_ws(' ',Address,City,ST,Zip)), '') != ''
-- UNION ALL
-- SELECT
--     DISTINCT 
--     TRIM(UPPER(md5_customer_val2)) AS src_id,
--     TRIM('105') AS src_cd,
--     TRIM(UPPER('867')) AS src_nm,
--     'HCO' AS entity_type,
--     MD5(UPPER(COALESCE(UPPER(TRIM(src_id)), 'NULL') || '|' || COALESCE(UPPER(TRIM(Address)), 'NULL')|| COALESCE(UPPER(TRIM(wholesaler)), 'NULL'))) AS addr_id,
--     TRIM(UPPER(Address)) AS addr_line1,
--     CAST(null AS STRING) as addr_line2,
--     TRIM(UPPER(City)) AS city,
--     TRIM(UPPER(ST)) AS state,
--     TRIM(UPPER('US')) AS country, 
--     CASE 
--         WHEN CHARINDEX('-', Zip) > 0 THEN LPAD(TRIM(RIGHT(Zip, 4)), 4, 0)
--         ELSE NULL
--     END AS zip4,
--     LPAD(TRIM(LEFT(Zip, 5)), 5, 0) AS zip5,
    
--     NULL AS addr_type, 
--     'Y' AS prim_addr_flg,
--     NULL as is_delete,
--     'A' AS address_status,  
--     NULL AS address_verification_status,
--     CAST(null AS STRING) AS attr_1,
--     CAST(null AS STRING) AS attr_2,
--     NULL AS delta_flg,
--     NULL AS mdfd_dt,
--     NULL AS crtd_dt,
--     NULL AS pt_data_dt
-- FROM spsd_867
-- WHERE coalesce(TRIM(concat_ws(' ',Address,City,ST,Zip)), '') != ''
"""
)
spsd_867_filtered_df = clean_zip(spsd_867_filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **MERGE**

# COMMAND ----------

# DBTITLE 1,Cell 4
xpo_filtered_df.createOrReplaceTempView("xpo_filtered_view")
veeva_filtered_df.createOrReplaceTempView('veeva_filtered_view')
ddd_filtered_df.createOrReplaceTempView("ddd_filtered_view")
spsd_867_filtered_df.createOrReplaceTempView("spsd_867_filtered_view")
merged_pty_df = spark.sql(f"""
    SELECT *,
        MD5(
            UPPER(
                COALESCE(UPPER(TRIM(addr_line1)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(city)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(state)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(country)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(zip4)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(zip5)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(addr_type)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(prim_addr_flg)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(address_status)), 'NULL') || '|' ||
                COALESCE(UPPER(TRIM(address_verification_status)), 'NULL') 
            )
        ) AS md5_val
    FROM 
    (
        SELECT * FROM xpo_filtered_view
        UNION 
        SELECT * FROM ddd_filtered_view
        UNION 
        SELECT * FROM veeva_filtered_view
        UNION
        SELECT * FROM spsd_867_filtered_view
    )
""")

# COMMAND ----------

merged_pty_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ## ACD 

# COMMAND ----------

# MAGIC %md
# MAGIC ### new acd with cast (Null)

# COMMAND ----------

source_catalog = "case_study_catalog.raw"
destination_catalog = "case_study_catalog.bronze"

merged_pty_df.createOrReplaceTempView("merged_pty_address_view")

# ✅ Check if table exists
table_exists = spark.catalog.tableExists(f"{destination_catalog}.stg_pty_address")

# ✅ ---------------- ADDED RECORDS ----------------
if not table_exists:
    added_pty_df = spark.sql(f"""
        SELECT
            m.src_id, m.src_cd, m.src_nm,
            m.addr_id, m.addr_line1,m.addr_line2, m.city, m.state, m.country,
            m.zip4, m.zip5,m.entity_type,

            CAST(m.addr_type AS STRING) AS addr_type,   

            m.prim_addr_flg, m.address_status, m.address_verification_status,
            m.attr_1,

            CAST(m.attr_2 AS STRING) AS attr_2,        

            CASE 
                WHEN UPPER(m.is_delete) = 'TRUE' THEN 'D' 
                ELSE 'A' 
            END AS delta_flg,

            m.md5_val,

            CAST(NULL AS TIMESTAMP) AS mdfd_dt,        

            CURRENT_TIMESTAMP() AS crtd_dt,
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS pt_data_dt

        FROM merged_pty_address_view m
    """)
else:
    added_pty_df = spark.sql(f"""
        SELECT
            m.src_id, m.src_cd, m.src_nm,
            m.addr_id, m.addr_line1,m.addr_line2, m.city, m.state, m.country,
            m.zip4, m.zip5,m.entity_type,

            CAST(m.addr_type AS STRING) AS addr_type,   

            m.prim_addr_flg, m.address_status, m.address_verification_status,
            m.attr_1,

            CAST(m.attr_2 AS STRING) AS attr_2,        

            CASE 
                WHEN UPPER(m.is_delete) = 'TRUE' THEN 'D' 
                ELSE 'A' 
            END AS delta_flg,

            m.md5_val,

            CAST(NULL AS TIMESTAMP) AS mdfd_dt,        

            CURRENT_TIMESTAMP() AS crtd_dt,
            DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS pt_data_dt

        FROM merged_pty_address_view m

        LEFT ANTI JOIN {destination_catalog}.stg_pty_address o
        ON TRIM(CONCAT(m.src_id, m.src_cd, COALESCE(m.addr_id,''))) =
           TRIM(CONCAT(o.src_id, o.src_cd, COALESCE(o.addr_id,'')))
    """)

added_pty_df.createOrReplaceTempView("added_pty_address_view")

# ✅ ---------------- CHANGED RECORDS ----------------
if table_exists:
    changed_pty_df = spark.sql(f"""
        SELECT
            m.src_id, m.src_cd, m.src_nm,
            m.addr_id, m.addr_line1,m.addr_line2, m.city, m.state, m.country,
            m.zip4, m.zip5,m.entity_type,

            CAST(m.addr_type AS STRING) AS addr_type,   

            m.prim_addr_flg, m.address_status, m.address_verification_status,
            m.attr_1,

            CAST(m.attr_2 AS STRING) AS attr_2,        

            CASE 
                WHEN UPPER(m.is_delete) = 'TRUE' THEN 'D' 
                ELSE 'C' 
            END AS delta_flg,

            m.md5_val,

            CURRENT_TIMESTAMP() AS mdfd_dt,          

            o.crtd_dt,
            DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS pt_data_dt

        FROM merged_pty_address_view m

        INNER JOIN {destination_catalog}.stg_pty_address o
        ON TRIM(CONCAT(m.src_id, m.src_cd, COALESCE(m.addr_id,''))) =
           TRIM(CONCAT(o.src_id, o.src_cd, COALESCE(o.addr_id,'')))

        AND TRIM(COALESCE(m.md5_val,'')) <>
            TRIM(COALESCE(o.md5_val,''))
    """)
else:
    changed_pty_df = spark.createDataFrame([], added_pty_df.schema)

changed_pty_df.createOrReplaceTempView("changed_pty_address_view")

# ✅ ---------------- UNCHANGED RECORDS ----------------
if table_exists:
    unchanged_df = spark.sql(f"""
        SELECT
            o.src_id, o.src_cd, o.src_nm,
            o.addr_id, o.addr_line1,o.addr_line2, o.city, o.state, o.country,
            o.zip4, o.zip5,o.entity_type,

            CAST(o.addr_type AS STRING) AS addr_type,  

            o.prim_addr_flg, o.address_status, o.address_verification_status,
            o.attr_1,

            CAST(o.attr_2 AS STRING) AS attr_2,       

            o.delta_flg,
            o.md5_val,

            CAST(o.mdfd_dt AS TIMESTAMP) AS mdfd_dt,   

            o.crtd_dt,
            o.pt_data_dt

        FROM {destination_catalog}.stg_pty_address o

        WHERE NOT EXISTS (
            SELECT 1
            FROM changed_pty_address_view ch
            WHERE TRIM(CONCAT(ch.src_id, ch.src_cd, COALESCE(ch.addr_id,''))) =
                  TRIM(CONCAT(o.src_id, o.src_cd, COALESCE(o.addr_id,'')))
        )
    """)
else:
    unchanged_df = spark.createDataFrame([], added_pty_df.schema)

unchanged_df.createOrReplaceTempView("unchanged_pty_address_view")

# ✅ ---------------- FINAL UNION ----------------
final_pty_address_df = spark.sql("""
SELECT DISTINCT
    src_id, src_cd, src_nm,
    addr_id, addr_line1,addr_line2, city, state, country,
    zip4, zip5, entity_type,addr_type,
    prim_addr_flg, address_status, address_verification_status,
    attr_1, attr_2,
    delta_flg, md5_val,
    mdfd_dt, crtd_dt, pt_data_dt

FROM (
    SELECT * FROM added_pty_address_view
    UNION
    SELECT * FROM changed_pty_address_view
    UNION
    SELECT * FROM unchanged_pty_address_view
)
""")
# ✅ STEP: Apply dedup + primary flag logic
# SELECT
#     DISTINCT
#     src_id, src_cd, src_nm,
#     addr_id, addr_line1, addr_line2, city, state, country,
#     zip4, zip5, entity_type, addr_type,

#     CASE 
#         WHEN addr_line1 IS NULL OR TRIM(addr_line1) = '' THEN 'N'   
#         WHEN rn = 1 THEN 'Y'
#         ELSE 'N'
#     END AS prim_addr_flg,

#     address_status,
#     address_verification_status,
#     attr_1, attr_2,
#     delta_flg, md5_val,
#     mdfd_dt, crtd_dt, pt_data_dt

# FROM (
#     SELECT *,
#         ROW_NUMBER() OVER (
#             PARTITION BY src_id, src_cd, addr_id   -- ❌ old partition
#             ORDER BY LENGTH(addr_line1) DESC       -- ❌ old ranking
#         ) AS rn
#     FROM final_base
# )


final_pty_address_df.createOrReplaceTempView("final_base")

final_pty_address_df = spark.sql("""

WITH enriched AS (
    SELECT *,
        
        -- ✅ Cleaned string
        TRIM(addr_line1) AS clean_addr,

        -- ✅ Count number of words (tokens)
        SIZE(SPLIT(TRIM(addr_line1), '\\\\s+')) AS word_count,

        -- ✅ Remove very small tokens (1-letter words) and measure meaningful length
        LENGTH(
            CONCAT_WS(' ',
                FILTER(
                    SPLIT(TRIM(addr_line1), '\\\\s+'),
                    x -> LENGTH(x) > 1
                )
            )
        ) AS meaningful_length

    FROM final_base
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY src_id, src_cd   -- ✅ correct partition
            ORDER BY

                -- ✅ 1. Valid addresses first
                CASE 
                    WHEN clean_addr IS NULL OR clean_addr = '' THEN 2
                    ELSE 1
                END,

                -- ✅ 2. More words preferred
                word_count DESC,

                -- ✅ 3. More meaningful content preferred
                meaningful_length DESC,

                -- ✅ 4. Final fallback
                LENGTH(clean_addr) DESC,
                clean_addr DESC

        ) AS rn
    FROM enriched
)

SELECT DISTINCT
    src_id, src_cd, src_nm,
    addr_id, addr_line1, addr_line2, city, state, country,
    zip4, zip5, entity_type, addr_type,

    CASE 
        WHEN addr_line1 IS NULL OR TRIM(addr_line1) = '' THEN 'N'
        WHEN rn = 1 THEN 'Y'
        ELSE 'N'
    END AS prim_addr_flg,

    address_status,
    address_verification_status,
    attr_1, attr_2,
    delta_flg, md5_val,
    mdfd_dt, crtd_dt, pt_data_dt

FROM ranked
""")
# ✅ ---------------- WRITE ----------------
final_pty_address_df.write.mode("overwrite") \
    .saveAsTable(f"{destination_catalog}.stg_pty_address")

# COMMAND ----------

# MAGIC %md
# MAGIC # DQM

# COMMAND ----------

destination_catalog = "case_study_catalog.bronze"
table_name = f"{destination_catalog}.stg_pty_address"

dq_all = spark.sql(f"""

-- ✅ 1. Address Line1 Empty (Non-critical)
SELECT 
    src_id, src_cd, src_nm,
    'UNKNOWN' AS entity_type,
    '80' AS qc_id,
    'stg_pty_address' AS tbl_nm,
    'addr_line1' AS col_nm,
    'Empty_Address_Line1' AS err_val,
    '0' AS crtly,
    pt_data_dt
FROM {table_name}
WHERE COALESCE(TRIM(addr_line1), '') = ''


UNION ALL

-- ✅ 2. ZIP5 Empty (Non-critical)
SELECT 
    src_id, src_cd, src_nm,
    'UNKNOWN' AS entity_type,
    '81' AS qc_id,
    'stg_pty_address' AS tbl_nm,
    'zip5' AS col_nm,
    'Empty_ZIP5' AS err_val,
    '0' AS crtly,
    pt_data_dt
FROM {table_name}
WHERE COALESCE(TRIM(zip5), '') = ''


UNION ALL

-- ✅ 3. ZIP5 Invalid Length (Non-critical)
SELECT 
    src_id, src_cd, src_nm,
    'UNKNOWN' AS entity_type,
    '82' AS qc_id,
    'stg_pty_address' AS tbl_nm,
    'zip5' AS col_nm,
    'Invalid_ZIP5_Length' AS err_val,
    '0' AS crtly,
    pt_data_dt
FROM {table_name}
WHERE COALESCE(TRIM(zip5), '') != '' 
  AND LENGTH(TRIM(zip5)) != 5

""")
dq_all.write.mode("append").saveAsTable(f"{destination_catalog}.dq_err_lg")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.bronze.stg_pty_address where src_id=addr_id;

# COMMAND ----------

# MAGIC %md
# MAGIC # Test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.bronze.stg_pty_address;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM merged_pty_address_view
# MAGIC WHERE zip4 RLIKE '^[0-9]{5}-[0-9]{4}$';

# COMMAND ----------



spark.sql("""
SELECT src_id, src_cd, addr_id, COUNT(*) cnt
FROM case_study_catalog.bronze.stg_pty_address
GROUP BY src_id, src_cd, addr_id
HAVING cnt > 3
""").display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.bronze.stg_pty_address where src_id='4854311'

# COMMAND ----------

# MAGIC %sql
# MAGIC select src_nm ,attr_1 from case_study_catalog.bronze.stg_pty_address where addr_line1 is null group by src_nm, attr_1;

# COMMAND ----------

spark.sql("""
TRUNCATE TABLE case_study_catalog.bronze.stg_pty_address
""")
