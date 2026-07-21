# Databricks notebook source
catalog = 'case_study_catalog'

# COMMAND ----------

pty_hco_id_with_dqm = spark.sql(f"""
    SELECT DISTINCT A.*,
    CASE
      WHEN B.crtly = '1'
        THEN 'C'
      WHEN B.crtly = '0'
        THEN 'NC'
      ELSE
        null
    END AS dqm_flg
    FROM 
        {catalog}.bronze.stg_pty_id A
    LEFT JOIN
        (select row_number() over (partition by concat(src_id, src_cd) order by crtly desc, pt_data_dt desc) as rn, * from {catalog}.bronze.dq_err_lg where  qc_id in (70, 71, 72,73) and entity_type = 'HCO') B
    ON
        trim(concat(A.src_id, A.src_cd)) = trim(concat(B.src_id, B.src_cd))
        AND B.rn = 1 
""")

pty_hco_id_with_dqm.createOrReplaceTempView("pty_hco_id_with_dqm_view")

pty_hco_id_with_eid = spark.sql(f"""
    SELECT CAST(B.cs_id AS STRING)  AS cs_id, A.*
    FROM 
        pty_hco_id_with_dqm_view A
    LEFT JOIN
        {catalog}.silver.hco_id_store B
    ON
        upper(trim(concat(A.src_id, A.src_cd))) = upper(trim(concat(B.src_id, B.src_cd))) where A.entity_type = 'HCO'
""")

pty_hco_id_with_eid.createOrReplaceTempView("pty_hco_id_with_eid_view")


# COMMAND ----------

# Creating dataframe having all the columns from the stage identifier table and adding dqm flag.

pty_hcp_id_with_dqm = spark.sql(f'''
    SELECT DISTINCT A.*,
    CASE
      WHEN B.crtly = '1'
        THEN 'C'
      WHEN B.crtly = '0'
        THEN 'NC'
      ELSE
        null
    END AS dqm_flg
    FROM 
        {catalog}.bronze.stg_pty_id A
    LEFT JOIN
        (select row_number() over (partition by concat(src_id, src_cd) order by crtly desc, pt_data_dt desc) as rn, * from {catalog}.bronze.dq_err_lg where  qc_id in (21, 22, 23) and entity_type = "HCP") B
    ON
        trim(concat(A.src_id, A.src_cd)) = trim(concat(B.src_id, B.src_cd))
        AND B.rn = 1 and B.entity_type = "HCP" 
''')


# pty_hcp_id_with_dqm.display()

pty_hcp_id_with_dqm.createOrReplaceTempView("pty_hcp_id_with_dqm_view")

pty_hcp_id_with_eid = spark.sql(f"""
    SELECT CAST(B.cs_id AS STRING)  AS cs_id, A.*
    FROM 
        pty_hcp_id_with_dqm_view A
    LEFT JOIN
        {catalog}.silver.hcp_id_store B
    ON
        upper(trim(concat(A.src_id, A.src_cd))) = upper(trim(concat(B.src_id, B.src_cd))) where A.entity_type = 'HCP'
""")
# pty_hcp_id_with_eid.display()

pty_hcp_id_with_eid.createOrReplaceTempView("pty_hcp_id_with_eid_view")


# COMMAND ----------

pty_id_view = spark.sql(f"""
    SELECT * FROM pty_hco_id_with_eid_view
    UNION ALL
    SELECT * FROM pty_hcp_id_with_eid_view
""")
pty_id_view.createOrReplaceTempView("pty_id_view")

# COMMAND ----------

#Overwrite the premdm identifier table.

pty_id_view.write.mode("overwrite").saveAsTable(f"{catalog}.silver.premdm_pty_id")