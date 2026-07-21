# Databricks notebook source
catalog = "case_study_catalog"

# COMMAND ----------

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_id WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type='HCO'""").createOrReplaceTempView("premdm_pty_id_hco_nc_view")

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_id WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type = 'HCP'""").createOrReplaceTempView("premdm_pty_id_hcp_nc_view")


# COMMAND ----------

spark.sql(f''' 
WITH RankedData AS (
    SELECT DISTINCT
        A.cs_id,
        B.id_val, 
        B.id_type,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY C.rank ASC, B.pt_data_dt DESC) AS rank_id_val
    FROM 
        {catalog}.gold.pub_pty_xref A
    LEFT JOIN 
        premdm_pty_id_hcp_nc_view B
    ON 
        TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))

    LEFT JOIN {catalog}.bronze.survivorship_table C ON UPPER(A.src_nm) = UPPER(C.source) AND C.attribute = 'id_val' AND    C.entity = 'HCP'
    WHERE
        TRIM(B.id_type) = "NPI" AND A.ety_type = 'HCP'
    
    UNION

    SELECT DISTINCT
        A.cs_id,
        B.id_val, 
        B.id_type,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY C.rank ASC, B.pt_data_dt DESC) AS rank_id_val
    FROM 
        {catalog}.gold.pub_pty_xref A
    LEFT JOIN 
        premdm_pty_id_hcp_nc_view B
    ON 
        TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))

    LEFT JOIN {catalog}.bronze.survivorship_table C ON UPPER(A.src_nm) = UPPER(C.source) AND C.attribute = 'id_val' AND    C.entity = 'HCP'
    WHERE
        TRIM(B.id_type) = "DEA" AND A.ety_type = 'HCP'
    
    UNION

    SELECT DISTINCT
        A.cs_id,
        B.id_val, 
        B.id_type,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY C.rank ASC, B.pt_data_dt DESC) AS rank_id_val
    FROM 
        {catalog}.gold.pub_pty_xref A
    LEFT JOIN 
        premdm_pty_id_hcp_nc_view B
    ON 
        TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))

    LEFT JOIN {catalog}.bronze.survivorship_table C ON UPPER(A.src_nm) = UPPER(C.source) AND C.attribute = 'id_val' AND    C.entity = 'HCP'
    WHERE
        TRIM(B.id_type) = "ME" AND A.ety_type = 'HCP'
),
FirstNonNull AS (
SELECT
    DISTINCT
    "HCP" as ety_type,
    cs_id,
    'IDENTIFIER' AS attribute_domain,
    id_type as attribute_type,
    FIRST_VALUE(id_val) IGNORE NULLS OVER (PARTITION BY cs_id,id_type ORDER BY rank_id_val) AS attribute_value, 
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt,
    ROW_NUMBER() OVER (
        PARTITION BY cs_id,id_type 
        ORDER BY 
            (CASE WHEN id_val IS NOT NULL THEN 1 ELSE 0 END) DESC
    ) AS row_num
        
FROM 
    RankedData    
)
SELECT DISTINCT
    ety_type,
    cs_id,
    attribute_domain,
    attribute_type,
    attribute_value,
    insrt_dt
FROM FirstNonNull
WHERE row_num = 1
ORDER BY cs_id



''').createOrReplaceTempView("ranked_hcp_id_view")

# COMMAND ----------


spark.sql(f''' 
WITH RankedData AS (
    SELECT DISTINCT
    A.cs_id,
    B.id_val, 
    B.id_type,
    ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY C.rank ASC, B.pt_data_dt DESC) AS rank_id_val
    FROM 
        {catalog}.gold.pub_pty_xref A
    LEFT JOIN 
        premdm_pty_id_hco_nc_view B
    ON 
        TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))

    LEFT JOIN {catalog}.bronze.survivorship_table C ON UPPER(A.src_nm) = UPPER(C.source) AND C.attribute = 'id_val' AND    C.entity = 'HCO'
    WHERE
        TRIM(B.id_type) = "DEA" AND A.ety_type = 'HCO'
    

    
    UNION

    SELECT DISTINCT
        A.cs_id,
        B.id_val, 
        B.id_type,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY C.rank ASC, B.pt_data_dt DESC) AS rank_id_val
    FROM 
        {catalog}.gold.pub_pty_xref A
    LEFT JOIN 
        premdm_pty_id_hco_nc_view B
    ON 
        TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))

    LEFT JOIN {catalog}.bronze.survivorship_table C ON UPPER(A.src_nm) = UPPER(C.source) AND C.attribute = 'id_val' AND    C.entity = 'HCO'
    WHERE
        TRIM(B.id_type) = "HIN" AND A.ety_type = 'HCO'

  

    UNION

    SELECT DISTINCT
        A.cs_id,
        B.id_val, 
        B.id_type,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY C.rank ASC, B.pt_data_dt DESC) AS rank_id_val
    FROM 
        {catalog}.gold.pub_pty_xref A
    LEFT JOIN 
        premdm_pty_id_hco_nc_view B
    ON 
        TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))

    LEFT JOIN {catalog}.bronze.survivorship_table C ON UPPER(A.src_nm) = UPPER(C.source) AND C.attribute = 'id_val' AND    C.entity = 'HCO'
    WHERE
        TRIM(B.id_type) = "NPI" AND A.ety_type = 'HCO'

),
FirstNonNull AS (
SELECT
    DISTINCT
    "HCO" as ety_type,
    cs_id,
    'IDENTIFIER' AS attribute_domain,
    id_type as attribute_type,
    FIRST_VALUE(id_val) IGNORE NULLS OVER (PARTITION BY cs_id,id_type ORDER BY rank_id_val) AS attribute_value, 
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt,
    ROW_NUMBER() OVER (
        PARTITION BY cs_id,id_type 
        ORDER BY 
            (CASE WHEN id_val IS NOT NULL THEN 1 ELSE 0 END) DESC
    ) AS row_num
        
FROM 
    RankedData    
)
SELECT DISTINCT
    ety_type,
    cs_id,
    attribute_domain,
    attribute_type,
    attribute_value,
    insrt_dt
FROM FirstNonNull
WHERE row_num = 1
ORDER BY cs_id

''').createOrReplaceTempView("ranked_hco_id_view")

# COMMAND ----------

pub_pty_identifier_df = spark.sql('''
    SELECT ety_type,cs_id,attribute_domain,attribute_type,attribute_value,insrt_dt FROM ranked_hcp_id_view
    UNION
    SELECT ety_type,cs_id,attribute_domain,attribute_type,attribute_value,insrt_dt FROM ranked_hco_id_view  
''')
# pub_pty_identifier_df.display()
pub_pty_identifier_df.createOrReplaceTempView("pub_pty_identifier_view")


# COMMAND ----------

final_df = spark.sql(f"""
    SELECT DISTINCT 
    ety_type,
    cs_id,
    attribute_domain,
    attribute_type,
    attribute_value,
    insrt_dt
    from pub_pty_identifier_view""")


# COMMAND ----------

final_df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.pub_pty_identifiers")