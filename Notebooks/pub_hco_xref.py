# Databricks notebook source
catalog = 'case_study_catalog'

# COMMAND ----------

# MAGIC %pip install jellyfish

# COMMAND ----------

from pyspark.sql import functions as F
from collections import defaultdict, deque
from pyspark.sql.types import *

# COMMAND ----------

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_hco WHERE COALESCE(dqm_flg, 'NC') = 'NC' """).createOrReplaceTempView("premdm_pty_hco_nc")

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_address WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type = 'HCO'""").createOrReplaceTempView("premdm_pty_address_hco_nc")

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_id WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type = 'HCO'""").createOrReplaceTempView("premdm_pty_id_hco_nc")


# COMMAND ----------

hco_filtered_df = spark.sql(f"""
SELECT 
  DISTINCT * 
FROM 
(
  SELECT DISTINCT 
    A.cs_id,
    A.src_id, 
    A.src_cd, 
    A.src_nm,
    A.full_nm,
    A.attr_1,
    C.id_val AS dea 
  FROM 
    premdm_pty_hco_nc A
  LEFT JOIN
    premdm_pty_id_hco_nc C
  ON 
    TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(C.src_id, C.src_cd))
    AND TRIM(C.id_type) = 'DEA'
)
WHERE 
--unc for attr limit
--   attr_1 = '1'
--   AND 
  (COALESCE(dea, '') != '')
""")

hco_filtered_df.createOrReplaceTempView("hco_filtered_view")

# COMMAND ----------

hco_filtered_df_match_idf = spark.sql(f"""
SELECT DISTINCT *
FROM (
    SELECT DISTINCT 
        A.cs_id AS id1, A.src_id, A.src_cd, A.src_nm AS src_nm_a, A.full_nm full_nm_a, A.dea AS a_idf_val, A.attr_1 AS level_a,
        B.cs_id AS id2, B.src_id, B.src_cd, B.src_nm AS src_nm_b, B.full_nm full_nm_b, B.dea AS b_idf_val, B.attr_1 AS level_b
    FROM hco_filtered_view A
    CROSS JOIN hco_filtered_view B
    ON COALESCE(TRIM(A.dea), '') = COALESCE(TRIM(B.dea), '') and A.attr_1 = B.attr_1
    WHERE A.cs_id <> B.cs_id 
      AND COALESCE(UPPER(A.dea), 'N/A') <> 'N/A'
      AND A.src_nm <> B.src_nm
)
""")

hco_filtered_df_match_idf.createOrReplaceTempView("hco_filtered_df_match_idf")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from premdm_pty_address_hco_nc

# COMMAND ----------

hco_filtered_df_exact = spark.sql(f"""
SELECT DISTINCT
    A.cs_id,
    A.src_id,
    A.src_cd,
    A.src_nm,
    UPPER(A.stan_full_nm) AS stan_full_nm,
    A.acnt_desc1 as tgt_accnt_desc_1,
    A.acnt_desc2 as tgt_accnt_desc_2,
    A.acnt_desc3 as tgt_accnt_desc_3,
    A.acnt_desc4 as tgt_accnt_desc_4,
    A.attr_1,
    B.addr_id,
    UPPER(B.stan_addr_line1) AS stan_addr_line1,
    B.addr_line2,
    UPPER(B.stan_city) AS stan_city,
    UPPER(B.stan_state) AS stan_state,
    B.zip4,
    B.stan_zip5 AS stan_zip5
FROM 
    premdm_pty_hco_nc A
LEFT JOIN
    premdm_pty_address_hco_nc B
ON 
    TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd)) 
    -- and A.attr_1 = B.attr_1
-- unc for attr limit
-- WHERE 
--     A.attr_1='1'
""")

hco_filtered_df_exact.createOrReplaceTempView("hco_filtered_df_exact_view")

# COMMAND ----------

hco_filtered_df_exact.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from hco_filtered_df_exact_view

# COMMAND ----------

from pyspark.sql.types import DoubleType
import jellyfish
def jaro_winkler_sim1(str1, str2):
    if str1 is None or str2 is None:
        return 0.0
    return float(jellyfish.jaro_winkler_similarity(str(str1), str(str2)))

spark.udf.register("jaro_winkler1", jaro_winkler_sim1, DoubleType())

# Execute the query
df = spark.sql(f"""
WITH analysis AS (
    SELECT DISTINCT 
        A.cs_id AS c_id_a, 
        A.src_nm AS src_nm_a, 
        A.stan_full_nm AS stan_full_nm_a, 
        A.stan_addr_line1 AS stan_addr_line1_a, 
        A.stan_city AS stan_city_a, 
        A.stan_state AS stan_state_a, 
        A.stan_zip5 AS stan_zip_a, 
        A.tgt_accnt_desc_4 AS tgt_accnt_desc_4_a,
        A.attr_1 AS level_a,
        B.cs_id AS c_id_b, 
        B.src_nm AS src_nm_b, 
        B.stan_full_nm AS stan_full_nm_b, 
        B.stan_addr_line1 AS stan_addr_line1_b, 
        B.stan_city AS stan_city_b, 
        B.stan_state AS stan_state_b, 
        B.stan_zip5 AS stan_zip_b, 
        B.tgt_accnt_desc_4 AS tgt_accnt_desc_4_b,
        B.attr_1 AS level_b
    FROM
        hco_filtered_df_exact_view A
    LEFT JOIN 
        hco_filtered_df_exact_view B
    ON
        jaro_winkler1(TRIM(UPPER(A.stan_full_nm)), TRIM(UPPER(B.stan_full_nm))) BETWEEN 0.90D AND 1.0D
        AND jaro_winkler1(TRIM(UPPER(A.stan_addr_line1)), TRIM(UPPER(B.stan_addr_line1))) BETWEEN 0.90D AND 1.0D
        AND TRIM(UPPER(A.stan_city)) = TRIM(UPPER(B.stan_city))
        AND TRIM(UPPER(A.stan_state)) = TRIM(UPPER(B.stan_state))
        AND TRIM(UPPER(A.stan_zip5)) = TRIM(UPPER(B.stan_zip5))
        AND A.attr_1 = B.attr_1
    WHERE
        A.cs_id < B.cs_id 
        AND A.src_nm <> B.src_nm


    UNION

    SELECT DISTINCT 
        A.cs_id AS c_id_a, 
        A.src_nm AS src_nm_a, 
        A.stan_full_nm AS stan_full_nm_a, 
        A.stan_addr_line1 AS stan_addr_line1_a, 
        A.stan_city AS stan_city_a, 
        A.stan_state AS stan_state_a, 
        A.stan_zip5 AS stan_zip_a, 
        A.tgt_accnt_desc_4 AS tgt_accnt_desc_4_a,
        A.attr_1 AS level_a,
        B.cs_id AS c_id_b, 
        B.src_nm AS src_nm_b, 
        B.stan_full_nm AS stan_full_nm_b, 
        B.stan_addr_line1 AS stan_addr_line1_b, 
        B.stan_city AS stan_city_b, 
        B.stan_state AS stan_state_b, 
        B.stan_zip5 AS stan_zip_b, 
        B.tgt_accnt_desc_4 AS tgt_accnt_desc_4_b,
        B.attr_1 AS level_b
    FROM
        hco_filtered_df_exact_view A
    LEFT JOIN 
        hco_filtered_df_exact_view B
    ON
        TRIM(UPPER(A.stan_addr_line1)) = TRIM(UPPER(B.stan_addr_line1))
        AND TRIM(UPPER(A.stan_city)) = TRIM(UPPER(B.stan_city))
        AND TRIM(UPPER(A.stan_state)) = TRIM(UPPER(B.stan_state))
        AND TRIM(UPPER(A.stan_zip5)) = TRIM(UPPER(B.stan_zip5))
        AND A.attr_1 = B.attr_1
    WHERE
        A.cs_id < B.cs_id 
        AND A.src_nm <> B.src_nm
       
)
SELECT DISTINCT * FROM analysis
""")

df.printSchema()
# df.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.temp_hco_fuzzy")

# df.createOrReplaceTempView('hco_filtered_df_match_fuzzy')
# display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.temp_hco_fuzzy")

df.createOrReplaceTempView('hco_filtered_df_match_fuzzy')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from case_study_catalog.bronze.temp_hco_fuzzy

# COMMAND ----------

matched_merged_df = spark.sql(f"""
    SELECT DISTINCT id1, id2 
    FROM hco_filtered_df_match_idf

    UNION

    SELECT DISTINCT c_id_a AS id1, c_id_b AS id2 
    FROM {catalog}.bronze.temp_hco_fuzzy

""")

matched_merged_df.createOrReplaceTempView("matched_merged_view")
# display(matched_merged_df)

# COMMAND ----------

import pandas as pd
from collections import defaultdict, deque

matched_merged_df_pd = matched_merged_df.toPandas()

# Build the adjacency list
graph = defaultdict(list)
for id1, id2 in zip(matched_merged_df_pd['id1'], matched_merged_df_pd['id2']):
    graph[id1].append(id2)
    graph[id2].append(id1)

# Function to find connected components and assign the lowest ID as group ID
def find_connected_components(graph):
    visited = set()
    components = []
    group_mapping = {}

    def bfs(node):
        queue = deque([node])
        component = []
        while queue:
            current = queue.popleft()
            if current not in visited:
                visited.add(current)
                component.append(current)
                queue.extend(graph[current])
        return component

    for node in graph:
        if node not in visited:
            component = bfs(node)
            components.append(component)
            # Assign the lowest ID as the group ID for this component
            group_id = min(component)
            for member in component:
                group_mapping[member] = group_id

    return group_mapping

# Find connected components and assign group IDs
group_mapping = find_connected_components(graph)

# Create a DataFrame for the result
result = [{'master_id': group_mapping[node], 'id': node} for node in group_mapping]
result_df = pd.DataFrame(result).sort_values(by=['master_id', 'id']).reset_index(drop=True)

# Display the final table
result_sp_df = spark.createDataFrame(result_df)
# result_sp_df = spark.createDataFrame(result)
# result_sp_df.display()
result_sp_df.createOrReplaceTempView("matched_merged_df_xref")

# COMMAND ----------

# Get singleton IDs
spark.sql("""
    SELECT DISTINCT cs_id 
    FROM premdm_pty_hco_nc 
    WHERE COALESCE(dqm_flg, 'NC') = 'NC' 
    EXCEPT
    SELECT id AS cs_id 
    FROM matched_merged_df_xref
""").createOrReplaceTempView("singleton_ids_view")

# Union of matched and singleton records
final_xref = spark.sql("""
                       

SELECT 
    DISTINCT 'HCO' AS ety_type, 
    xref.master_id AS cs_id, 
    xref.id AS unique_src_id, 
    hco.src_id, 
    hco.src_cd, 
    hco.src_nm,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt,
    'Y' AS ismatched 
FROM matched_merged_df_xref xref
LEFT JOIN premdm_pty_hco_nc hco 
    ON xref.id = hco.cs_id 
-- WHERE xref.id <> xref.master_id
--

UNION

SELECT 
    DISTINCT 'HCO' AS ety_type, 
    xref.cs_id AS cs_id, 
    xref.cs_id AS unique_src_id, 
    hco.src_id, 
    hco.src_cd, 
    hco.src_nm,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt,
    'N' AS ismatched
FROM singleton_ids_view xref
LEFT JOIN premdm_pty_hco_nc hco 
    ON xref.cs_id = hco.cs_id
    
""")

# final_xref.display()

final_xref.createOrReplaceTempView("final_xref_view")

# COMMAND ----------

final_xref.write.mode("append").saveAsTable(f"{catalog}.gold.pub_pty_xref")