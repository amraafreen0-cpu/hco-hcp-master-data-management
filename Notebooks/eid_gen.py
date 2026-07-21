# Databricks notebook source
catalog = "case_study_catalog"

# COMMAND ----------

# Filter out all those accounts which dont have EID yet

acc_not_having_eid = spark.sql(f"""
select DISTINCT
    A.src_id, 
    A.src_nm, 
    A.src_cd
from 
    {catalog}.bronze.stg_pty_hco as A
left join 
    {catalog}.silver.hco_id_store as B
on 
    upper(trim(concat(A.src_nm,A.src_id))) = upper(trim(concat(B.src_nm,B.src_id))) 
where 
    B.cs_id is null
""")
acc_not_having_eid.createOrReplaceTempView("acc_not_having_eid_view")


incoming_data_df = spark.sql("SELECT COUNT(*) as row_count FROM acc_not_having_eid_view")
row_count = incoming_data_df.collect()[0]['row_count']


if row_count > 0:

    provide_eid = spark.sql(f""" 
    WITH New_Data AS (
        -- new incoming data
        SELECT
            src_id,
            src_nm,
            src_cd,
            ROW_NUMBER() OVER (ORDER by src_nm, src_id) AS rn
        FROM 
            acc_not_having_eid_view
    ),
    Max_Cs_Id AS (
        -- Get the maximum  from the master table
        SELECT 
            COALESCE(MAX(cs_id), 19999999) AS max_id
        FROM 
            {catalog}.silver.hco_id_store
    )
    -- Assign incremental IDs to the new data
    INSERT INTO {catalog}.silver.hco_id_store (cs_id, src_id, src_cd,src_nm)
    SELECT 
        max_id + rn AS cs_id,
        src_id,
        src_cd,
        src_nm
    FROM 
        New_Data, Max_cs_id;
    """)
else:
    print("No new data")

# COMMAND ----------

# Filter out all those accounts which dont have EID yet

acc_not_having_eid = spark.sql(f"""
select DISTINCT
    A.src_id, 
    A.src_nm, 
    A.src_cd
from 
    {catalog}.bronze.stg_pty_hcp as A
left join 
    {catalog}.silver.hcp_id_store as B
on 
    upper(trim(concat(A.src_nm,A.src_id))) = upper(trim(concat(B.src_nm,B.src_id))) 
where 
    B.cs_id is null
""")
acc_not_having_eid.createOrReplaceTempView("acc_not_having_eid_view")


incoming_data_df = spark.sql("SELECT COUNT(*) as row_count FROM acc_not_having_eid_view")
row_count = incoming_data_df.collect()[0]['row_count']


if row_count > 0:

    provide_eid = spark.sql(f""" 
    WITH New_Data AS (
        -- new incoming data
        SELECT
            src_id,
            src_nm,
            src_cd,
            ROW_NUMBER() OVER (ORDER by src_nm, src_id) AS rn
        FROM 
            acc_not_having_eid_view
    ),
    Max_Cs_Id AS (
        -- Get the maximum  from the master table
        SELECT 
            COALESCE(MAX(cs_id), 9999999) AS max_id
        FROM 
            {catalog}.silver.hcp_id_store
    )
    -- Assign incremental IDs to the new data
    INSERT INTO {catalog}.silver.hcp_id_store (cs_id, src_id, src_cd,src_nm)
    SELECT 
        max_id + rn AS cs_id,
        src_id,
        src_cd,
        src_nm
    FROM 
        New_Data, Max_cs_id;
    """)
else:
    print("No new data")