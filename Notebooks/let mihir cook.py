# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select cs_id, count(*) from case_study_catalog.gold.pub_pty_xref group by 1 order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select A.*, B.cs_id 
# MAGIC from case_study_catalog.silver.premdm_pty_address A 
# MAGIC left join case_study_catalog.gold.pub_pty_xref B 
# MAGIC   on A.cs_id = B.unique_src_id 
# MAGIC where B.cs_id in (
# MAGIC   select cs_id 
# MAGIC   from case_study_catalog.gold.pub_pty_xref 
# MAGIC   group by cs_id 
# MAGIC   having count(*) > 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from case_study_catalog.raw.crm_address where id in ('ADDR757', 'ADDR2429','ADDR2333')