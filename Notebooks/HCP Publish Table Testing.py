# Databricks notebook source
# MAGIC %md
# MAGIC # HCP Party Publish Testing
# MAGIC ## Testing the following:
# MAGIC ### 1) Celltrion ID NULL Check
# MAGIC Ensure that no Celltrion IDs are null in the dataset.
# MAGIC ### 2) Referential Integrity: All Master IDs Must Be Present in PTY_XREF
# MAGIC Verify that all master IDs exist in the PTY_XREF table.
# MAGIC ### 3) Name Survivorship
# MAGIC Check that the name fields are correctly populated and consistent.
# MAGIC ### 4) Address Line 1 Survivorship
# MAGIC Ensure that the primary address line 1 is correctly populated and consistent.
# MAGIC ### 5) State Survivorship
# MAGIC Verify that the state information is correctly populated and consistent.
# MAGIC ### 6) City Survivorship
# MAGIC Ensure that the city information is correctly populated and consistent.
# MAGIC ### 7) ZIP Code Survivorship
# MAGIC Check that the ZIP code information is correctly populated and consistent.
# MAGIC ### 8) Single Primary Address Check
# MAGIC Ensure that there is only one primary address per HCP.
# MAGIC ### 9) ZIP Code Length Check
# MAGIC Verify that the ZIP code length is appropriate.
# MAGIC ### 10) State Value Check
# MAGIC Ensure that the state contains no special characters.
# MAGIC ### 11) Numeric ZIP Code
# MAGIC Ensure that the ZIP code contains only numeric values.
# MAGIC ### 12) No Critical Failed Records in Pub Pty Xref
# MAGIC Check that there are no critical failed records in the Pub Pty Xref table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.gold.pub_pty_hcp where cs_id is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct cs_id from case_study_catalog.gold.pub_pty_hcp;
# MAGIC select distinct cs_id from case_study_catalog.gold.pub_pty_xref where ety_type='HCP'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.bronze.survivorship_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return Mismatched records if any

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         A.cs_id, 
# MAGIC         B.cs_id AS master_id, 
# MAGIC         A.src_nm, 
# MAGIC         A.full_nm 
# MAGIC     FROM 
# MAGIC        case_study_catalog.silver.premdm_pty_hcp A
# MAGIC     LEFT JOIN (
# MAGIC         SELECT * 
# MAGIC         FROM case_study_catalog.gold.pub_pty_xref 
# MAGIC         WHERE ety_type = 'HCP'
# MAGIC     ) B ON A.cs_id = B.cs_id
# MAGIC     WHERE A.src_nm <> 'OVERRIDE'
# MAGIC ),
# MAGIC rank_cte AS (
# MAGIC     SELECT 
# MAGIC         *, 
# MAGIC         row_number() OVER (
# MAGIC             PARTITION BY master_id 
# MAGIC             ORDER BY 
# MAGIC                 UPPER(src_nm) = 'ONEKEY' DESC, 
# MAGIC                 UPPER(src_nm) = 'VEEVA' DESC, 
# MAGIC                 UPPER(src_nm) = 'XPONENT' DESC, 
# MAGIC                 UPPER(src_nm) = '867' DESC, 
# MAGIC                 cs_id
# MAGIC         ) AS rank
# MAGIC     FROM 
# MAGIC         base_cte
# MAGIC ),
# MAGIC comp_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         master_id, 
# MAGIC         A.full_nm AS curr_full_nm,
# MAGIC         B.src_nm AS calc_src_nm,
# MAGIC         B.full_nm AS calc_full_nm,
# MAGIC         CASE 
# MAGIC             WHEN A.full_nm = B.full_nm THEN 'Y' 
# MAGIC             ELSE 'N' 
# MAGIC         END AS is_correct
# MAGIC     FROM 
# MAGIC         case_study_catalog.gold.pub_pty_hcp A
# MAGIC     INNER JOIN 
# MAGIC         (SELECT * FROM rank_cte WHERE rank = 1 AND master_id IS NOT NULL) B 
# MAGIC     ON 
# MAGIC         A.cs_id = B.master_id    
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM comp_cte 
# MAGIC WHERE is_correct = 'N'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4

# COMMAND ----------

# MAGIC %md
# MAGIC ###PRIMARY ADDRESS SURVIVORSHIP VALIDATION
# MAGIC IGNORE THE N'S AS CURRENT ADDRESS IS MUCH BETTER THAN STAN ADDRESS

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         A.cs_id AS master_id, 
# MAGIC         B.cs_id, 
# MAGIC         A.src_nm, 
# MAGIC         B.stan_addr_line1 
# MAGIC     FROM (
# MAGIC         SELECT * 
# MAGIC         FROM case_study_catalog.gold.pub_pty_xref
# MAGIC         WHERE ety_type = 'HCP'
# MAGIC     ) A
# MAGIC     LEFT JOIN case_study_catalog.silver.premdm_pty_address B 
# MAGIC         ON A.unique_src_id = B.cs_id
# MAGIC     WHERE B.prim_addr_flg = 'Y'
# MAGIC ),
# MAGIC rank_cte AS (
# MAGIC     SELECT 
# MAGIC         *, 
# MAGIC         row_number() OVER (
# MAGIC             PARTITION BY master_id 
# MAGIC             ORDER BY 
# MAGIC                 CASE 
# MAGIC                     WHEN UPPER(src_nm) = 'ONEKEY' THEN 1
# MAGIC                     WHEN UPPER(src_nm) = 'XPONENT' THEN 2
# MAGIC                     WHEN UPPER(src_nm) = 'VEEVA' THEN 3
# MAGIC                     WHEN UPPER(src_nm) = '867' THEN 4
# MAGIC                     ELSE 5
# MAGIC                 END
# MAGIC         ) AS rank
# MAGIC     FROM 
# MAGIC         base_cte
# MAGIC ),
# MAGIC comp_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         master_id, 
# MAGIC         A.addr_line1 AS curr_addr_line1,
# MAGIC         B.src_nm AS calc_src_nm,
# MAGIC         UPPER(B.stan_addr_line1) AS calc_addr_line1,
# MAGIC         CASE 
# MAGIC             WHEN TRIM(COALESCE(A.addr_line1, '')) = TRIM(UPPER(COALESCE(B.stan_addr_line1, ''))) THEN 'Y'
# MAGIC             ELSE 'N' 
# MAGIC         END AS is_correct
# MAGIC     FROM 
# MAGIC        case_study_catalog.gold.pub_pty_hcp A
# MAGIC     INNER JOIN (
# MAGIC         SELECT * 
# MAGIC         FROM rank_cte 
# MAGIC         WHERE rank = 1 
# MAGIC           AND master_id IS NOT NULL
# MAGIC     ) B ON A.cs_id = B.master_id
# MAGIC     WHERE A.primary_address = 'Y'
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM comp_cte 
# MAGIC WHERE is_correct = 'N'

# COMMAND ----------

# MAGIC %sql
# MAGIC select stan_addr_line1,addr_line1,src_id,src_nm from case_study_catalog.silver.premdm_pty_address where stan_addr_line1= '1083 CEDAR CT';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5

# COMMAND ----------

# MAGIC %md
# MAGIC ###PRIMARY STATE SURVIVORSHIP VALIDATION

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         A.cs_id AS master_id, 
# MAGIC         B.cs_id, 
# MAGIC         A.src_nm, 
# MAGIC         B.stan_state
# MAGIC     FROM (
# MAGIC         SELECT * 
# MAGIC         FROM case_study_catalog.gold.pub_pty_xref
# MAGIC         WHERE ety_type = 'HCP'
# MAGIC     ) A
# MAGIC     LEFT JOIN case_study_catalog.silver.premdm_pty_address B 
# MAGIC         ON A.unique_src_id = B.cs_id
# MAGIC     WHERE B.prim_addr_flg = 'Y'
# MAGIC ),
# MAGIC rank_cte AS (
# MAGIC     SELECT 
# MAGIC         *, 
# MAGIC         row_number() OVER (
# MAGIC             PARTITION BY master_id 
# MAGIC             ORDER BY 
# MAGIC                 CASE 
# MAGIC                     WHEN UPPER(src_nm) = 'ONEKEY' THEN 1
# MAGIC                     WHEN UPPER(src_nm) = 'XPONENT' THEN 2
# MAGIC                     WHEN UPPER(src_nm) = 'VEEVA' THEN 3
# MAGIC                     WHEN UPPER(src_nm) = '867' THEN 4
# MAGIC                     ELSE 5
# MAGIC                 END
# MAGIC         ) AS rank
# MAGIC     FROM 
# MAGIC         base_cte
# MAGIC ),
# MAGIC comp_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         master_id, 
# MAGIC         A.state AS curr_state,
# MAGIC         B.src_nm AS calc_src_nm,
# MAGIC         UPPER(B.stan_state) AS casl_state,
# MAGIC         CASE 
# MAGIC             WHEN TRIM(COALESCE(A.state, '')) = TRIM(UPPER(COALESCE(B.stan_state, ''))) THEN 'Y'
# MAGIC             ELSE 'N' 
# MAGIC         END AS is_correct
# MAGIC     FROM 
# MAGIC        case_study_catalog.gold.pub_pty_hcp A
# MAGIC     INNER JOIN (
# MAGIC         SELECT * 
# MAGIC         FROM rank_cte 
# MAGIC         WHERE rank = 1 
# MAGIC           AND master_id IS NOT NULL
# MAGIC     ) B ON A.cs_id = B.master_id
# MAGIC     WHERE A.primary_address = 'Y'
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM comp_cte 
# MAGIC WHERE is_correct = 'N'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6

# COMMAND ----------

# MAGIC %md
# MAGIC ### City

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         A.cs_id AS master_id, 
# MAGIC         B.cs_id, 
# MAGIC         A.src_nm, 
# MAGIC         B.stan_city
# MAGIC     FROM (
# MAGIC         SELECT * 
# MAGIC         FROM case_study_catalog.gold.pub_pty_xref
# MAGIC         WHERE ety_type = 'HCP'
# MAGIC     ) A
# MAGIC     LEFT JOIN case_study_catalog.silver.premdm_pty_address B 
# MAGIC         ON A.unique_src_id = B.cs_id
# MAGIC     WHERE B.prim_addr_flg = 'Y'
# MAGIC ),
# MAGIC rank_cte AS (
# MAGIC     SELECT 
# MAGIC         *, 
# MAGIC         row_number() OVER (
# MAGIC             PARTITION BY master_id 
# MAGIC             ORDER BY 
# MAGIC                 CASE 
# MAGIC                     WHEN UPPER(src_nm) = 'ONEKEY' THEN 1
# MAGIC                     WHEN UPPER(src_nm) = 'XPONENT' THEN 2
# MAGIC                     WHEN UPPER(src_nm) = 'VEEVA' THEN 3
# MAGIC                     WHEN UPPER(src_nm) = '867' THEN 4
# MAGIC                     ELSE 5
# MAGIC                 END
# MAGIC         ) AS rank
# MAGIC     FROM 
# MAGIC         base_cte
# MAGIC ),
# MAGIC comp_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         master_id, 
# MAGIC         A.city   AS curr_city,
# MAGIC         B.src_nm AS calc_src_nm,
# MAGIC         UPPER(B.stan_city) AS casl_city,
# MAGIC         CASE 
# MAGIC             WHEN TRIM(COALESCE(A.city, '')) = TRIM(UPPER(COALESCE(B.stan_city, ''))) THEN 'Y'
# MAGIC             ELSE 'N' 
# MAGIC         END AS is_correct
# MAGIC     FROM 
# MAGIC        case_study_catalog.gold.pub_pty_hcp A
# MAGIC     INNER JOIN (
# MAGIC         SELECT * 
# MAGIC         FROM rank_cte 
# MAGIC         WHERE rank = 1 
# MAGIC           AND master_id IS NOT NULL
# MAGIC     ) B ON A.cs_id = B.master_id
# MAGIC     WHERE A.primary_address = 'Y'
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM comp_cte 
# MAGIC WHERE is_correct = 'N'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7

# COMMAND ----------

# MAGIC %md
# MAGIC ### ZIP5

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         A.cs_id AS master_id, 
# MAGIC         B.cs_id, 
# MAGIC         A.src_nm, 
# MAGIC         B.stan_zip5
# MAGIC     FROM (
# MAGIC         SELECT * 
# MAGIC         FROM case_study_catalog.gold.pub_pty_xref
# MAGIC         WHERE ety_type = 'HCP'
# MAGIC     ) A
# MAGIC     LEFT JOIN case_study_catalog.silver.premdm_pty_address B 
# MAGIC         ON A.unique_src_id = B.cs_id
# MAGIC     WHERE B.prim_addr_flg = 'Y'
# MAGIC ),
# MAGIC rank_cte AS (
# MAGIC     SELECT 
# MAGIC         *, 
# MAGIC         row_number() OVER (
# MAGIC             PARTITION BY master_id 
# MAGIC             ORDER BY 
# MAGIC                 CASE 
# MAGIC                     WHEN UPPER(src_nm) = 'ONEKEY' THEN 1
# MAGIC                     WHEN UPPER(src_nm) = 'XPONENT' THEN 2
# MAGIC                     WHEN UPPER(src_nm) = 'VEEVA' THEN 3
# MAGIC                     WHEN UPPER(src_nm) = '867' THEN 4
# MAGIC                     ELSE 5
# MAGIC                 END
# MAGIC         ) AS rank
# MAGIC     FROM 
# MAGIC         base_cte
# MAGIC ),
# MAGIC comp_cte AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         master_id, 
# MAGIC         A.city   AS curr_city,
# MAGIC         B.src_nm AS calc_src_nm,
# MAGIC         UPPER(B.stan_zip5) AS casl_city,
# MAGIC         CASE 
# MAGIC             WHEN TRIM(COALESCE(A.zip5, '')) = TRIM(UPPER(COALESCE(B.stan_zip5, ''))) THEN 'Y'
# MAGIC             ELSE 'N' 
# MAGIC         END AS is_correct
# MAGIC     FROM 
# MAGIC        case_study_catalog.gold.pub_pty_hcp A
# MAGIC     INNER JOIN (
# MAGIC         SELECT * 
# MAGIC         FROM rank_cte 
# MAGIC         WHERE rank = 1 
# MAGIC           AND master_id IS NOT NULL
# MAGIC     ) B ON A.cs_id = B.master_id
# MAGIC     WHERE A.primary_address = 'Y'
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM comp_cte 
# MAGIC WHERE is_correct = 'N'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8

# COMMAND ----------

# MAGIC %sql
# MAGIC select cs_id, count(primary_address) as cnt from case_study_catalog.gold.pub_pty_hcp
# MAGIC where primary_address = 'Y'
# MAGIC group by cs_id
# MAGIC having cnt>1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.gold.pub_pty_hcp where length(zip5) <> 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.gold.pub_pty_hcp where state RLIKE '[^a-zA-Z ]'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 11

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from case_study_catalog.gold.pub_pty_hcp where zip5 RLIKE '[^0-9]'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 12

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT 
# MAGIC     A.src_id, 
# MAGIC     A.src_nm, 
# MAGIC     B.unique_src_id, 
# MAGIC     B.cs_id 
# MAGIC FROM 
# MAGIC     (SELECT * 
# MAGIC      FROM case_study_catalog.bronze.dq_err_lg 
# MAGIC      WHERE crtly = '1' 
# MAGIC        AND entity_type = 'HCP') A 
# MAGIC INNER JOIN 
# MAGIC     (SELECT * 
# MAGIC      FROM case_study_catalog.gold.pub_pty_xref) B 
# MAGIC ON 
# MAGIC     CONCAT(A.src_id, A.src_cd) = CONCAT(B.src_id, B.src_cd)

# COMMAND ----------

# MAGIC %md
# MAGIC # HCP Publish Identifier Testing
# MAGIC ## Testing the following:
# MAGIC ### 1) Referential Integrity:
# MAGIC Verify that every master ID exists in the `pub_pty_hcp` table.
# MAGIC
# MAGIC ### 2) Referential Integrity:
# MAGIC Ensure that every master ID is found in the `pub_pty_xref` table.
# MAGIC
# MAGIC ### 3) Identifier Validation:
# MAGIC Check that the identifiers table contains only identifiers, specialty, and communication data.
# MAGIC
# MAGIC ### 4) Identifier Completeness:
# MAGIC Ensure that both NPI and DEA identifiers are available for each HCP.
# MAGIC
# MAGIC ### 5) Identifier Null Check:
# MAGIC Verify that neither the NPI nor HIN fields are null.
# MAGIC
# MAGIC ### 6) Specialty Completeness:
# MAGIC Check that each HCP has both primary and secondary specialties recorded.
# MAGIC
# MAGIC ### 7) Specialty Null Check:
# MAGIC Ensure that neither the primary nor the secondary specialty fields are null.
# MAGIC
# MAGIC ### 8) Communication Data Validation:
# MAGIC Verify that the communication data includes only fax, email, and phone entries.
# MAGIC
# MAGIC ### 9) Communication Data Null Check:
# MAGIC Ensure that fax, email, and phone fields are not null.
# MAGIC
# MAGIC ### 10) NPI Uniqueness:
# MAGIC Check that each Celltrion master ID has a unique NPI.
# MAGIC
# MAGIC ### 11) NPI Duplication Check:
# MAGIC Ensure that no NPI is linked to more than one Celltrion ID.
# MAGIC
# MAGIC ### 12) NPI Length Check:
# MAGIC Verify that the NPI length is exactly 10 characters.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 1
# MAGIC
# MAGIC select * from case_study_catalog.gold.pub_pty_identifiers where ety_type = 'HCP' and cs_id not in 
# MAGIC (
# MAGIC   select distinct cs_id from case_study_catalog.gold.pub_pty_hcp
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test 1
# MAGIC
# MAGIC select * from case_study_catalog.gold.pub_pty_identifiers where ety_type = 'HCP' and cs_id not in 
# MAGIC (
# MAGIC   select distinct cs_id from case_study_catalog.gold.pub_pty_xref where ety_type = 'HCP'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test 3

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct attribute_domain from case_study_catalog.gold.pub_pty_identifiers where ety_type = 'HCP'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct attribute_type from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_domain = 'IDENTIFIER'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_domain = 'IDENTIFIER' and attribute_value is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct attribute_type from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_domain = 'SPECIALTY'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_domain = 'SPECIALTY' and attribute_value is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8

# COMMAND ----------

# MAGIC %md
# MAGIC we dont have communication

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct attribute_type from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_domain = 'COMMUNICATION'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_domain = 'COMMUNICATION' and attribute_value is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct cs_id, count(distinct attribute_value) cnt
# MAGIC from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_type = 'NPI'
# MAGIC group by all
# MAGIC having cnt > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 11

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct attribute_value, count(distinct cs_id) cnt
# MAGIC from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_type = 'NPI'
# MAGIC group by all
# MAGIC having cnt > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 12

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct length(attribute_value)
# MAGIC from case_study_catalog.gold.pub_pty_identifiers 
# MAGIC where ety_type = 'HCP' and attribute_type = 'NPI'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Affliatons are not created yet

# COMMAND ----------

# MAGIC %md
# MAGIC # HCP Publish Affiliation Testing
# MAGIC ## Testing the following:
# MAGIC ### 1) NULL Check
# MAGIC Ensure no null values in the `child_id` column.
# MAGIC
# MAGIC ### 2) NULL Check
# MAGIC Ensure no null values in the `parent_id` column.
# MAGIC
# MAGIC ### 3) Referential Integrity
# MAGIC Verify that all `child_id` values exist in the `PTY_XREF` table.
# MAGIC
# MAGIC ### 4) Referential Integrity
# MAGIC Verify that all `parent_id` values exist in the `PTY_XREF` table.
# MAGIC
# MAGIC ### 5) Single Primary Affiliation
# MAGIC Check that each `child_id` has only one primary `parent_id`.
# MAGIC
# MAGIC ### 6) Circular Affiliation
# MAGIC Check for any circular affiliations in the table.
# MAGIC
# MAGIC ### 7) Only HCO as Parent
# MAGIC Ensure that each `child_id` has only one primary `parent_id`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation where child_id is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation where parent_id is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct child_id from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation
# MAGIC minus
# MAGIC select distinct celltrion_id from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_xref

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct parent_id from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation
# MAGIC minus
# MAGIC select distinct celltrion_id from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_xref

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select child_id, count(distinct parent_id) as cnt
# MAGIC from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation
# MAGIC where primary_affiliation = 'Y'
# MAGIC group by child_id
# MAGIC having cnt>1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation A 
# MAGIC inner join us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation B
# MAGIC on A.child_id = B.parent_id and A.parent_id = B.child_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from us_cdp_databricks_mdm_prod.comm_gold.pub_pty_affiliation where parent_id < '20000000'