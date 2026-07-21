# Databricks notebook source
catalog = 'case_study_catalog'

# COMMAND ----------

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_hcp WHERE COALESCE(dqm_flg, 'NC') = 'NC' """).createOrReplaceTempView("premdm_pty_hcp_nc_view")

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_address WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type = 'HCP'""").createOrReplaceTempView("premdm_pty_address_hcp_nc_view")


# COMMAND ----------

# Matching HCP table based on survivorship table

pty_hcp_survived_df = spark.sql(f"""
WITH RankedData AS (
SELECT DISTINCT

        A.cs_id,

        A.src_nm,

        B.stan_pty_type,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY C.rank ASC, B.pt_data_dt DESC) AS rank_pty_type,
      

        B.first_nm ,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY G.rank ASC, B.pt_data_dt DESC) AS rank_first_nm,

        B.middle_nm,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY H.rank ASC, B.pt_data_dt DESC) AS rank_middle_nm,

        B.last_nm,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY I.rank ASC, B.pt_data_dt DESC) AS rank_last_nm,

        B.full_nm,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY J.rank ASC, B.pt_data_dt DESC) AS rank_full_nm,

        B.gndr_cd,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY K.rank ASC, B.pt_data_dt DESC) AS rank_gndr_cd,

        B.kaiser_flg,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY L.rank ASC, B.pt_data_dt DESC) AS rank_kaiser_flg,

        B.stan_pty_status,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY U.rank ASC, B.pt_data_dt DESC) AS rank_stan_pty_status,

        B.pty_strsn_cd,

        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY V.rank ASC, B.pt_data_dt DESC) AS rank_pty_strsn_cd

    FROM

        {catalog}.gold.pub_pty_xref a

    LEFT JOIN

         premdm_pty_hcp_nc_view B  

    ON

        TRIM(CONCAT(A.unique_src_id)) = TRIM(CONCAT(B.cs_id))

 

    LEFT JOIN {catalog}.bronze.survivorship_table C ON UPPER(A.src_nm) = UPPER(C.source) AND C.attribute = 'hcp_type' AND C.entity = 'HCP'


    LEFT JOIN {catalog}.bronze.survivorship_table G ON UPPER(A.src_nm) = UPPER(G.source) AND G.attribute = 'first_nm' AND G.entity = 'HCP'

    LEFT JOIN {catalog}.bronze.survivorship_table H ON UPPER(A.src_nm) = UPPER(H.source) AND H.attribute = 'middle_nm' AND H.entity = 'HCP'

    LEFT JOIN {catalog}.bronze.survivorship_table I ON UPPER(A.src_nm) = UPPER(I.source) AND I.attribute = 'last_nm' AND I.entity = 'HCP'

    LEFT JOIN {catalog}.bronze.survivorship_table J ON UPPER(A.src_nm) = UPPER(J.source) AND J.attribute = 'full_nm' AND J.entity = 'HCP'

    LEFT JOIN {catalog}.bronze.survivorship_table K ON UPPER(A.src_nm) = UPPER(K.source) AND K.attribute = 'gndr_cd' AND K.entity = 'HCP'

    LEFT JOIN {catalog}.bronze.survivorship_table L ON UPPER(A.src_nm) = UPPER(L.source) AND L.attribute = 'kaiser_flg' AND L.entity = 'HCP'


    LEFT JOIN {catalog}.bronze.survivorship_table U ON UPPER(A.src_nm) = UPPER(U.source) AND U.attribute = 'stan_pty_status' AND U.entity = 'HCP'

    LEFT JOIN {catalog}.bronze.survivorship_table V ON UPPER(A.src_nm) = UPPER(V.source) AND V.attribute = 'pty_strsn_cd' AND V.entity = 'HCP'

    WHERE

        A.ety_type = 'HCP'
),
FirstNonNull AS (
    SELECT DISTINCT
        cs_id,
        FIRST_VALUE(stan_pty_type) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_pty_type) AS stan_pty_type,
        FIRST_VALUE(first_nm) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_first_nm) AS first_nm,
        FIRST_VALUE(middle_nm) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_middle_nm) AS middle_nm,
        FIRST_VALUE(last_nm) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_last_nm) AS last_nm,
        FIRST_VALUE(full_nm) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_full_nm) AS full_nm,
        FIRST_VALUE(gndr_cd) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_gndr_cd) AS gndr_cd,
        FIRST_VALUE(kaiser_flg) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_kaiser_flg) AS kaiser_flg,
        FIRST_VALUE(stan_pty_status) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_stan_pty_status) AS stan_pty_status,
        FIRST_VALUE(pty_strsn_cd) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_pty_strsn_cd) AS pty_strsn_cd,
        ROW_NUMBER() OVER (
            PARTITION BY cs_id 
            ORDER BY 
                (CASE WHEN stan_pty_type IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN first_nm IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN middle_nm IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN last_nm IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN full_nm IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN gndr_cd IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN kaiser_flg IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN stan_pty_status IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN pty_strsn_cd IS NOT NULL THEN 1 ELSE 0 END) DESC
        ) AS row_num
    FROM RankedData
)
SELECT DISTINCT
    cs_id,
    stan_pty_type as pty_type,
    first_nm,
    middle_nm,
    last_nm,
    full_nm,
    gndr_cd,
    kaiser_flg,
    stan_pty_status as pty_status,
    pty_strsn_cd
FROM FirstNonNull
WHERE row_num = 1
ORDER BY cs_id
""")

# pty_hcp_survived_df.display()

pty_hcp_survived_df.createOrReplaceTempView("pty_hcp_survived_view")

# COMMAND ----------

spark.sql(f"""
        SELECT 
        DISTINCT A.cs_id,
        B.addr_id,
        B.stan_addr_line1 as addr_line1,
        B.addr_line2,
        B.stan_city as city,
        B.stan_state as state,
        B.zip4,
        B.stan_zip5 as zip5,
        B.stan_country as country,
        B.addr_type,
        B.prim_addr_flg,
        B.stan_address_status AS address_status,
        B.address_verification_status 
        FROM 
            {catalog}.silver.premdm_pty_address B
        LEFT JOIN
            {catalog}.gold.pub_pty_xref A            
        ON
            TRIM(CONCAT(UPPER(A.src_id), A.src_cd)) = TRIM(CONCAT(UPPER(B.src_id), B.src_cd))
        WHERE 
            A.ety_type = 'HCP' and B.entity_type = 'HCP'
        AND
            COALESCE(prim_addr_flg, 'N') = 'N'
""").createOrReplaceTempView("unchanged_hcp_address_survived_view")

# spark.sql("""SELECT * FROM unchanged_hcp_address_survived_view""").display()

# COMMAND ----------

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_address WHERE prim_addr_flg = 'Y' and entity_type = 'HCP'""").createOrReplaceTempView("changed_hcp_address_survived_view")

# spark.sql("""SELECT * FROM changed_hcp_address_survived_view""").display()

# COMMAND ----------

display(spark.sql(f"""select * from changed_hcp_address_survived_view"""))

# COMMAND ----------

# Matching HCP table based on survivorship table
hcp_address_survived_df = spark.sql(f"""
WITH RankedAddresses AS (
    SELECT DISTINCT
        A.cs_id,
        B.src_nm,
        B.addr_id,
        B.stan_addr_line1,
        B.addr_line2,
        B.stan_city,
        B.stan_state,
        B.stan_zip5,
        B.stan_country,
        B.prim_addr_flg,
        B.zip4,
        B.addr_type,
        B.stan_address_status AS address_status,
        B.address_verification_status,
        S.rank AS src_rank,
        ROW_NUMBER() OVER (
            PARTITION BY A.cs_id
            ORDER BY
                S.rank ASC,                       -- 1st priority: Source Rank (lower is better)
                LENGTH(B.stan_addr_line1) DESC,  -- 2nd priority: Longer address preferred
                B.cs_id ASC               -- 3rd priority: Lowest celltrion_id as tiebreaker
        ) AS rn
    FROM {catalog}.gold.pub_pty_xref A
    INNER JOIN changed_hcp_address_survived_view B
        ON A.unique_src_id = B.cs_id
    LEFT JOIN (
        SELECT *
        FROM {catalog}.bronze.survivorship_table
        WHERE entity = 'HCP' AND attribute = 'prim_addr_flg'
    ) S
        ON UPPER(TRIM(A.src_nm)) = UPPER(TRIM(S.source))
    WHERE A.ety_type = 'HCP'
)

SELECT DISTINCT
    cs_id,
    addr_id,
    stan_addr_line1 AS addr_line1,
    addr_line2,
    stan_city AS city,
    stan_state AS state,
    zip4,
    stan_zip5 AS zip5,
    stan_country AS country,
    addr_type,
    prim_addr_flg,
    address_status AS address_status,
    address_verification_status
FROM RankedAddresses
WHERE rn = 1
ORDER BY cs_id
""")


hcp_address_survived_df.createOrReplaceTempView("hcp_address_survived_view")



# COMMAND ----------

spark.sql("""
    SELECT 
        DISTINCT cs_id,
        UPPER(TRIM(addr_id)) as addr_id,
        UPPER(TRIM(addr_line1)) as addr_line1,
        UPPER(TRIM(addr_line2)) as addr_line2,
        UPPER(TRIM(city)) as city,
        UPPER(TRIM(state)) as state,
        UPPER(TRIM(zip4)) as zip4,
        CASE WHEN LENGTH(UPPER(TRIM(zip5))) <> 5 THEN NULL
        ELSE UPPER(TRIM(zip5)) end as zip5,
        UPPER(TRIM(country)) as country,
        UPPER(TRIM(addr_type)) as addr_type,
        UPPER(TRIM(prim_addr_flg)) as prim_addr_flg,
        UPPER(TRIM(address_status)) as address_status,
        UPPER(TRIM(address_verification_status)) as address_verification_status 
        FROM 
        unchanged_hcp_address_survived_view

    UNION

    SELECT 
        DISTINCT cs_id,
        UPPER(TRIM(addr_id)) as addr_id,
        UPPER(TRIM(addr_line1)) as addr_line1,
        UPPER(TRIM(addr_line2)) as addr_line2,
        UPPER(TRIM(city)) as city,
        UPPER(TRIM(state)) as state,
        UPPER(TRIM(zip4)) as zip4,
        CASE WHEN LENGTH(UPPER(TRIM(zip5))) <> 5 THEN NULL
        ELSE UPPER(TRIM(zip5)) end as zip5,
        UPPER(TRIM(country)) as country,
        UPPER(TRIM(addr_type)) as addr_type,
        UPPER(TRIM(prim_addr_flg)) as prim_addr_flg,
        UPPER(TRIM(address_status)) as address_status,
        UPPER(TRIM(address_verification_status)) as address_verification_status        
        FROM
        hcp_address_survived_view 
            
""").createOrReplaceTempView("hcp_address_final_view")

# COMMAND ----------

display(spark.sql(f"""select * from hcp_address_final_view"""))

# COMMAND ----------


pub_pty_hcp_df = spark.sql("""
    SELECT
        DISTINCT CAST(A.cs_id AS STRING) as cs_id,
       
        A.pty_type AS hcp_type,
        A.first_nm, 
        A.last_nm, 
        A.middle_nm,
        A.full_nm, 
        A.gndr_cd AS gender, 
        A.kaiser_flg, 
        A.pty_status,
        A.pty_strsn_cd,
        B.addr_id, 
        B.address_status, 
        B.addr_line1,
        B.addr_line2,
        B.city,
        B.state, 
        B.zip4,
        B.zip5, 
        B.prim_addr_flg AS primary_address,
        B.addr_type AS address_type,
        DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt
    FROM
        pty_hcp_survived_view A
    LEFT JOIN
        hcp_address_final_view B
    ON
        A.cs_id = B.cs_id
""")

# pub_pty_hcp_df.display()


# COMMAND ----------

display(pub_pty_hcp_df)

# COMMAND ----------

pub_pty_hcp_df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.pub_pty_hcp")
