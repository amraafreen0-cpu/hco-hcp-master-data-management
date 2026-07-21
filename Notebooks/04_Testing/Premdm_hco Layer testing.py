# Databricks notebook source
# MAGIC %md
# MAGIC For HCO, full_nm, cs_id and src_id should not be null
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.silver.premdm_pty_hco;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC SUM(CASE WHEN cs_id IS NULL THEN 1 ELSE 0 END) cs_id_nulls,
# MAGIC SUM(CASE WHEN src_id IS NULL THEN 1 ELSE 0 END) src_id_nulls,
# MAGIC SUM(CASE WHEN full_nm IS NULL THEN 1 ELSE 0 END) full_nm_nulls
# MAGIC FROM case_study_catalog.silver.premdm_pty_hco;

# COMMAND ----------

# MAGIC %md
# MAGIC # Record Count Staging vs PreMDM Layer
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM (
# MAGIC     SELECT src_id
# MAGIC     FROM case_study_catalog.bronze.stg_pty_hco
# MAGIC
# MAGIC     MINUS
# MAGIC
# MAGIC     SELECT src_id
# MAGIC     FROM case_study_catalog.silver.premdm_pty_hco
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # Null Check
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cs_id,
# MAGIC COUNT(*)
# MAGIC FROM case_study_catalog.silver.premdm_pty_hco
# MAGIC GROUP BY cs_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Source Mapping Validation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT src_cd,
# MAGIC COUNT(DISTINCT src_nm)
# MAGIC FROM case_study_catalog.silver.premdm_pty_hco
# MAGIC GROUP BY src_cd
# MAGIC HAVING COUNT(DISTINCT src_nm) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##There must be unique EID - cs_id for all the src_id, src_cd combination

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select cs_id, count(concat(src_id,src_cd)) as cnt from case_study_catalog.silver.premdm_pty_hco group by cs_id having count(concat(src_id,src_cd))>1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organization Name Distinct count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC stan_full_nm,
# MAGIC COUNT(*)
# MAGIC FROM case_study_catalog.silver.premdm_pty_hco
# MAGIC GROUP BY stan_full_nm
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY COUNT(*) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC stan_full_nm,
# MAGIC COUNT(DISTINCT src_cd)
# MAGIC FROM case_study_catalog.silver.premdm_pty_hco
# MAGIC GROUP BY stan_full_nm
# MAGIC HAVING COUNT(DISTINCT src_cd) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     md5_val,
# MAGIC     src_nm,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM case_study_catalog.silver.premdm_pty_hco
# MAGIC GROUP BY md5_val, src_nm
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM case_study_catalog.silver.premdm_pty_hco
# MAGIC WHERE md5_val = '1c0400390458c16df33dd12234fcae7c';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.silver.premdm_pty_hco where cs_id ='20002231';

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat(src_id,src_cd) from case_study_catalog.silver.premdm_pty_hco group by  concat(src_id,src_cd) having count(distinct src_cd)>1

# COMMAND ----------

# MAGIC %md
# MAGIC Above issue is due to duplicates created due to multiple DDD outlets name for same outlet_number.

# COMMAND ----------

# MAGIC %md
# MAGIC length of stan zip must be 5 or null

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct length(stan_zip5) from case_study_catalog.silver.premdm_pty_address_hco;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct length(stan_zip5) from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_address_hco;

# COMMAND ----------

# Multiple NPI at same Active HCP

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select A.celltrion_id,B.id_val as npi from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_hcp A left join us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_id_hcp B on A.celltrion_id = B.celltrion_id and B.id_type = 'NPI'
# MAGIC )
# MAGIC
# MAGIC select celltrion_id from cte group by celltrion_id having count(npi) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_hco where concat(src_id,src_cd) in (
# MAGIC select concat(src_id,src_cd) from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_hco
# MAGIC group by concat(src_id,src_cd) having count( distinct full_nm)>1);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_address_hcp where addr_id = '7276339'

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select A.celltrion_id,B.addr_id from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_hcp A left join us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_address_hcp B on A.celltrion_id = B.celltrion_id 
# MAGIC )
# MAGIC
# MAGIC select addr_id from cte group by addr_id having count(distinct celltrion_id) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_silver.premdm_pty_address_hcp  where addr_id = '0007293729'
