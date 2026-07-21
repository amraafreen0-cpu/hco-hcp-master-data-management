# Databricks notebook source
catalog = 'case_study_catalog'

# COMMAND ----------

# MAGIC %md
# MAGIC # Join with CS ID table

# COMMAND ----------


# Code to bring in the cs_id for delta records
pty_hco_with_eid = spark.sql(f"""
    SELECT DISTINCT B.cs_id  AS cs_id, A.*
    FROM 
        {catalog}.bronze.stg_pty_hco A
    LEFT JOIN
        {catalog}.silver.hco_id_store B
    ON
        upper(trim(concat(A.src_id, A.src_cd))) = upper(trim(concat(B.src_id, B.src_cd)))
""")

pty_hco_with_eid.createOrReplaceTempView("pty_hco_with_eid_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # Join Error Log

# COMMAND ----------

pty_hco_with_dqm = spark.sql(f"""
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
        pty_hco_with_eid_view A
    LEFT JOIN
        (SELECT ROW_NUMBER() over (partition by concat(src_id, src_cd) order by crtly desc, pt_data_dt desc) as rn, * from {catalog}.bronze.dq_err_lg where qc_id in (50, 51,52) and entity_type = "HCO") B
    ON
        TRIM(concat(A.src_id, A.src_cd)) = TRIM(concat(B.src_id, B.src_cd))
        AND B.entity_type = "HCO"
        AND B.rn = 1
""")
pty_hco_with_dqm.createOrReplaceTempView("pty_hco_with_dqm_view")


# COMMAND ----------

# MAGIC %md
# MAGIC # Standardization

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

# Load the hco_name_stand table 
hco_name_stand_df = spark.sql(f"SELECT * FROM {catalog}.bronze.hco_name_stand")

# Create a dictionary of mappings from unstandardized to standardized names
mappings = {row["unstand_hco_name"].lower(): row["stand_hco_name"] for row in hco_name_stand_df.collect()}

# Define a custom function to replace standalone words
def replace_standalone_word(full_nm):
    if full_nm is None:
        return None
    words = full_nm.lower().split()
    for i, word in enumerate(words):
        if word in mappings:
            words[i] = mappings[word]
    return " ".join(words)

# Register the UDF
replace_standalone_word_udf = F.udf(replace_standalone_word, StringType())

# Apply the custom function to the DataFrame
pty_hco_with_stan_hco_name = pty_hco_with_dqm.withColumn(
    "stan_full_nm", replace_standalone_word_udf(F.col("full_nm"))
)

# Replace digits, hyphens, and whitespace with space
pty_hco_with_stan_hco_name = pty_hco_with_dqm.withColumn(
    "stan_full_nm", F.regexp_replace(F.col("full_nm"), "[^A-Za-z0-9\\s]", " ")
)

# Normalize multiple spaces into a single space and trim leading/trailing spaces
pty_hco_with_stan_hco_name = pty_hco_with_stan_hco_name.withColumn(
    "stan_full_nm", F.upper(F.regexp_replace(F.col("stan_full_nm"), r"\s+", " "))
)

pty_hco_with_stan_hco_name = pty_hco_with_stan_hco_name.withColumn(
    "stan_full_nm", F.upper(F.trim(F.col("stan_full_nm")))
)

# Create a temporary view for further analysis
pty_hco_with_stan_hco_name.createOrReplaceTempView("pty_hco_with_stan_hco_name_view")

# COMMAND ----------

stan_pty_hco_df = spark.sql("""

SELECT DISTINCT
    cs_id,
    src_id,
    src_cd,
    src_nm,
    full_nm,
    stan_full_nm,
    acnt_cd1,
    acnt_cd2,
    acnt_cd3,
    acnt_cd4,
    acnt_desc1,
    acnt_desc2,
    acnt_desc3,
    acnt_desc4,
     pty_status,
    "A" as stan_pty_status,
    pty_strsn_cd,
    kaiser_flg,
    attr_1,
    attr_2,
    md5_val,
    delta_flg,
    mdfd_dt,
    crtd_dt,
    pt_data_dt,
    dqm_flg
FROM pty_hco_with_stan_hco_name_view
""")
# stan_pty_hco_df.display()
stan_pty_hco_df.createOrReplaceTempView("stan_pty_hco_view")

# COMMAND ----------

stan_pty_hco_df.write.mode("overwrite").saveAsTable(f"{catalog}.silver.premdm_pty_hco")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select md5_val, count(*) from case_study_catalog.silver.premdm_pty_hcp group by 1 order by 2 desc