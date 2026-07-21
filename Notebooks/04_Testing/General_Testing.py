# Databricks notebook source
# DBTITLE 1,xref testing
# MAGIC %sql
# MAGIC --HCO
# MAGIC select a.cs_id master_id, b.cs_id, b.src_nm, b.full_nm hco_name, c.stan_addr_line1, c.stan_city, c.stan_state, c.stan_zip5, d.id_val DEA, e.id_val HIN, b.attr_1
# MAGIC from case_study_catalog.gold.pub_pty_xref a
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_hco) b 
# MAGIC on a.unique_src_id = b.cs_id
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_address where prim_addr_flg = 'Y') c
# MAGIC on b.cs_id = c.cs_id
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_id where id_type = 'DEA') d
# MAGIC on b.cs_id = d.cs_id
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_id where id_type = 'HIN') e
# MAGIC on b.cs_id = e.cs_id
# MAGIC where a.ety_type = 'HCO'
# MAGIC
# MAGIC --HCP
# MAGIC select a.cs_id master_id, b.cs_id, b.src_nm, b.full_nm hcp_name, c.stan_addr_line1, c.stan_city, c.stan_state, c.stan_zip5, d.id_val NPI
# MAGIC from case_study_catalog.gold.pub_pty_xref a
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_hcp) b 
# MAGIC on a.unique_src_id = b.cs_id
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_address where prim_addr_flg = 'Y') c
# MAGIC on b.cs_id = c.cs_id
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_id where id_type = 'NPI') d
# MAGIC on b.cs_id = d.cs_id
# MAGIC where a.ety_type = 'HCP'
# MAGIC
# MAGIC
# MAGIC
# MAGIC select a.*, b.* from
# MAGIC (select a.cs_id id_a, b.id_val npi, c.cs_id master_id from 
# MAGIC case_study_catalog.silver.premdm_pty_hcp a
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_id where id_type = 'NPI') b
# MAGIC on a.cs_id = b.cs_id
# MAGIC left join case_study_catalog.gold.pub_pty_xref c
# MAGIC on a.cs_id = c.unique_src_id
# MAGIC where c.ety_type = 'HCP') a
# MAGIC left join 
# MAGIC (select a.cs_id id_a, b.id_val npi, c.cs_id master_id from 
# MAGIC case_study_catalog.silver.premdm_pty_hcp a
# MAGIC left join (select * from case_study_catalog.silver.premdm_pty_id where id_type = 'NPI') b
# MAGIC on a.cs_id = b.cs_id
# MAGIC left join case_study_catalog.gold.pub_pty_xref c
# MAGIC on a.cs_id = c.unique_src_id
# MAGIC where c.ety_type = 'HCP') b
# MAGIC on a.npi = b.npi
# MAGIC where a.master_id <> b.master_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select iqvia_pres_no,count(iqvia_pres_no) as cnt from case_study_catalog.raw.xponent_hcp group by iqvia_pres_no;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.bronze.stg_pty_hcp where src_id='4707584';
