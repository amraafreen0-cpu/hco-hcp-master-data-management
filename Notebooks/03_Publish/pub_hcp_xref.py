# Databricks notebook source
catalog = 'case_study_catalog'

# COMMAND ----------

# MAGIC %md
# MAGIC # HCP XREF

# COMMAND ----------

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_hcp WHERE COALESCE(dqm_flg, 'NC') = 'NC' """).createOrReplaceTempView("premdm_pty_hcp_nc")

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_address WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type = 'HCP' """).createOrReplaceTempView("premdm_pty_address_hcp_nc")

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_id WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type = 'HCP' """).createOrReplaceTempView("premdm_pty_id_hcp_nc")


# COMMAND ----------

hcp_master_df_1 = spark.sql(f"""
SELECT
    DISTINCT A.cs_id,
    A.src_id, A.src_cd, A.src_nm, A.full_nm, 
    C.id_val AS npi
FROM 
    premdm_pty_hcp_nc A
LEFT JOIN 
    premdm_pty_id_hcp_nc C
ON
    TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(C.src_id, C.src_cd))
    AND C.id_type = 'NPI'
""")


hcp_master_df_1.createOrReplaceTempView("hcp_master_view_1")

# COMMAND ----------

hcp_npi_match_df = spark.sql("""
SELECT 
    A.cs_id AS id1, 
    A.src_nm, 
    A.npi as npi_1,
    B.cs_id AS id2, 
    B.src_nm, 
    B.npi as npi_2
FROM 
    hcp_master_view_1 A
CROSS JOIN 
    hcp_master_view_1 B
ON 
    TRIM(A.npi) = TRIM(B.npi)
WHERE 
    A.cs_id <> B.cs_id
""")

# hcp_npi_match_df.display()

hcp_npi_match_df.createOrReplaceTempView("hcp_npi_match_view")

# COMMAND ----------

matched_merged_df = spark.sql(f"""
    SELECT DISTINCT id1, id2 FROM hcp_npi_match_view
""")

# matched_merged_df.display()

# matched_merged_df.createOrReplaceTempView("matched_merged_df")

# COMMAND ----------

import pandas as pd
from collections import defaultdict, deque

matched_merged_df_pd = matched_merged_df.toPandas()

graph = defaultdict(list)
for id1, id2 in zip(matched_merged_df_pd['id1'], matched_merged_df_pd['id2']):
    graph[id1].append(id2)
    graph[id2].append(id1)

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
            group_id = min(component)
            for member in component:
                group_mapping[member] = group_id

    return group_mapping

# Find connected components and assign group IDs
group_mapping = find_connected_components(graph)

# Create a DataFrame for the result
result = [{'master': group_mapping[node], 'id': node} for node in group_mapping]
result_df = pd.DataFrame(result).sort_values(by=['master', 'id']).reset_index(drop=True)

# Display the final table
result_sp_df = spark.createDataFrame(result_df)

# result_sp_df.display()

result_sp_df.createOrReplaceTempView("matched_merged_df_xref")


# COMMAND ----------

# Get singleton IDs
spark.sql("""
    SELECT cs_id 
    FROM premdm_pty_hcp_nc
    EXCEPT
    SELECT id AS cs_id 
    FROM matched_merged_df_xref
""").createOrReplaceTempView("singleton_ids_view")

# Union of matched and singleton records
final_xref = spark.sql("""
    SELECT 
        'HCP' AS ety_type, 
        xref.master AS cs_id, 
        xref.id AS unique_src_id, 
        HCP.src_id, 
        HCP.src_cd, 
        HCP.src_nm,
        DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt,
        'Y' AS ismatched 
    FROM matched_merged_df_xref xref
    LEFT JOIN premdm_pty_hcp_nc HCP 
        ON xref.id = HCP.cs_id
    UNION
    SELECT 
        'HCP' AS ety_type, 
        xref.cs_id AS master_id, 
        xref.cs_id AS celltrion_id, 
        HCP.src_id, 
        HCP.src_cd, 
        HCP.src_nm,
        DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt,
        'N' AS ismatched 
    FROM singleton_ids_view xref
    LEFT JOIN premdm_pty_hcp_nc HCP 
        ON xref.cs_id = HCP.cs_id
""")

# Display the final result
# final_xref.display()

# COMMAND ----------

final_xref.write.mode("overwrite").saveAsTable(f"{catalog}.gold.pub_pty_xref")
