# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM case_study_catalog.bronze.stg_pty_address
# MAGIC WHERE zip5 RLIKE '^[0-9]{5}-[0-9]{4}$'

# COMMAND ----------

# MAGIC %sql
# MAGIC select src_nm ,attr_1 from case_study_catalog.bronze.stg_pty_hcp where attr_1 is null group by src_nm,attr_1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.bronze.dq_err_lg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select entity_type,count( entity_type ) as cnt from case_study_catalog.bronze.stg_pty_address group by entity_type;

# COMMAND ----------

