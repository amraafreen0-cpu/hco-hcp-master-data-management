# Databricks notebook source
catalog = 'case_study_catalog'

# COMMAND ----------

# MAGIC %md
# MAGIC # Standardization

# COMMAND ----------

stan_pty_hcp_df = spark.sql(f"""
    SELECT DISTINCT 
        A.src_id, A.src_cd, A.src_nm, 
        pty_type,
        COALESCE(B.tgt_val, NULL) AS stan_pty_type,
        COALESCE(gndr_cd, 'U') AS gndr_cd, first_nm, middle_nm, last_nm, full_nm,
        pty_status,
        COALESCE(C.tgt_val, NULL) AS stan_pty_status,
        "MOCK STAND" AS pty_strsn_cd,
        kaiser_flg, attr_1, attr_2,
        md5_val, delta_flg, mdfd_dt, crtd_dt, pt_data_dt 
    FROM 
        {catalog}.bronze.stg_pty_hcp A
    LEFT JOIN 
        {catalog}.bronze.stand_map_master B
    ON 
        TRIM(A.pty_type) = TRIM(B.src_val)
        AND TRIM(B.stan_type) = 'pty_type'
    LEFT JOIN 
        {catalog}.bronze.stand_map_master C
    ON 
        TRIM(A.pty_status) = TRIM(C.src_val)
        AND TRIM(C.stan_type) = 'pty_status'
""")

# stan_pty_hcp_df.display()

stan_pty_hcp_df.createOrReplaceTempView("stan_pty_hcp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN WITH DQ LOG AND CS ID

# COMMAND ----------

# Code to bring the delta flag for delta records
pty_hcp_with_dqm = spark.sql(f"""
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
        stan_pty_hcp_view A
    LEFT JOIN
        (SELECT ROW_NUMBER() over (partition by concat(src_id, src_cd) order by crtly desc, pt_data_dt desc) as rn, * from {catalog}.bronze.dq_err_lg where qc_id in (10) AND entity_type = "HCP") B
    ON
        trim(concat(A.src_id, A.src_cd)) = trim(concat(B.src_id, B.src_cd))
        AND B.entity_type = "HCP"
        AND B.rn = 1
""")


# pty_hcp_with_dqm.display()

pty_hcp_with_dqm.createOrReplaceTempView("pty_hcp_with_dqm_view")

# Code to bring in the cs_id for delta records
pty_hcp_with_eid = spark.sql(f"""
    SELECT DISTINCT B.cs_id  AS cs_id, A.*
    FROM 
        pty_hcp_with_dqm_view A
    LEFT JOIN
        {catalog}.silver.hcp_id_store B
    ON
        upper(trim(concat(A.src_id, A.src_cd))) = upper(trim(concat(B.src_id, B.src_cd)))
""")


# pty_hcp_with_eid.display()

pty_hcp_with_eid.createOrReplaceTempView("pty_hcp_with_eid_view")



# COMMAND ----------

pty_hcp_with_eid.write.mode("overwrite").saveAsTable(f"{catalog}.silver.premdm_pty_hcp")
