# Databricks notebook source
# MAGIC %md
# MAGIC For hcp first name / full name should not be null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.silver.premdm_pty_hcp;

# COMMAND ----------

# MAGIC %md
# MAGIC birth_dt  must contain only birth year.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dt_of_brth from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_hcp 

# COMMAND ----------

# MAGIC %md
# MAGIC There must be unique EID for all the src_id,src_cd combination

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select cs_id, count(concat(src_id,src_cd)) as cnt from case_study_catalog.silver.premdm_pty_hcp group by cs_id having count(concat(src_id,src_cd))>1;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select concat(src_id,src_cd) from case_study_catalog.silver.premdm_pty_hcp group by  concat(src_id,src_cd) having count(distinct cs_id)>1

# COMMAND ----------

# MAGIC %md
# MAGIC Above issue is due to duplicates created due to multiple DDD outlets name for same outlet_number.

# COMMAND ----------

# MAGIC %md
# MAGIC length of stan zip must be 5 or null

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct length(stan_zip5) from case_study_catalog.silver.premdm_pty_address;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct length(stan_zip5) from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_address_hco;

# COMMAND ----------

# Multiple NPI at same Active HCP

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE case_study_catalog.silver.premdm_pty_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select B.cs_id,A.id_val as npi from case_study_catalog.silver.premdm_pty_hcp B left join case_study_catalog.silver.premdm_pty_id A on A.cs_id = B.cs_id and A.id_type = 'NPI'
# MAGIC )
# MAGIC
# MAGIC select cs_id from cte group by cs_id having count(npi) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  case_study_catalog.silver.premdm_pty_hcp where concat(src_id,src_cd) in (
# MAGIC select concat(src_id,src_cd) from case_study_catalog.silver.premdm_pty_hcp
# MAGIC group by concat(src_id,src_cd) having count( distinct full_nm)>1);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_address_hcp where addr_id = '7276339'

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select A.cs_id,B.addr_id from case_study_catalog.silver.premdm_pty_hcp A left join case_study_catalog.silver.premdm_pty_address B on A.cs_id = B.cs_id 
# MAGIC )
# MAGIC
# MAGIC select addr_id from cte group by addr_id having count(distinct cs_id) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_address_hcp  where addr_id = '0007293729'
