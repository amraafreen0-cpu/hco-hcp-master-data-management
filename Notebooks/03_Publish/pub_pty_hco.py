# Databricks notebook source
catalog = 'case_study_catalog'

# COMMAND ----------

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_hco WHERE COALESCE(dqm_flg, 'NC') = 'NC'""").createOrReplaceTempView("premdm_pty_hco_nc")

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_address WHERE COALESCE(dqm_flg, 'NC') = 'NC' and entity_type = 'HCO'""").createOrReplaceTempView("premdm_pty_address_hco_nc")


# COMMAND ----------

# Matching HCO table based on survivorship table

pty_hco_survived_df = spark.sql(f"""
WITH RankedData AS (
    SELECT DISTINCT
        A.cs_id,
        B.acnt_desc4 as tgt_accnt_desc_4,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY D.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_tgt_accnt_desc_4,        
       B.acnt_desc3 as tgt_accnt_desc_3,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY F.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_tgt_accnt_desc_3,
       B.acnt_desc2 as tgt_accnt_desc_2,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY H.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_tgt_accnt_desc_2,
      B.acnt_desc1 as tgt_accnt_desc_1,
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY J.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_tgt_accnt_desc_1,
        B.stan_full_nm, 
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY K.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_full_nm,
        B.kaiser_flg, 
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY L.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_kaiser_flg,
        B.stan_pty_status, 
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY M.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_pty_status,
        B.pty_strsn_cd, 
        ROW_NUMBER() OVER (PARTITION BY A.cs_id ORDER BY N.rank ASC,B.pt_data_dt desc, B.cs_id ASC) AS rank_pty_strsn_cd
    FROM 
        {catalog}.gold.pub_pty_xref A
    LEFT JOIN 
        premdm_pty_hco_nc B
    ON 
        TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))
    LEFT JOIN {catalog}.bronze.survivorship_table D ON UPPER(A.src_nm) = UPPER(D.source) AND D.entity = 'HCO' AND D.attribute = 'acnt_desc4'
    LEFT JOIN {catalog}.bronze.survivorship_table F ON UPPER(A.src_nm) = UPPER(F.source) AND F.entity = 'HCO' AND F.attribute = 'acnt_desc3'
    LEFT JOIN {catalog}.bronze.survivorship_table H ON UPPER(A.src_nm) = UPPER(H.source) AND H.entity = 'HCO' AND H.attribute = 'acnt_desc2'
    LEFT JOIN {catalog}.bronze.survivorship_table J ON UPPER(A.src_nm) = UPPER(J.source) AND J.entity = 'HCO' AND J.attribute = 'acnt_desc1'
    LEFT JOIN {catalog}.bronze.survivorship_table K ON UPPER(A.src_nm) = UPPER(K.source) AND K.entity = 'HCO' AND K.attribute = 'full_nm'
    LEFT JOIN {catalog}.bronze.survivorship_table L ON UPPER(A.src_nm) = UPPER(L.source) AND L.entity = 'HCO' AND L.attribute = 'kaiser_flg'
    LEFT JOIN {catalog}.bronze.survivorship_table M ON UPPER(A.src_nm) = UPPER(M.source) AND M.entity = 'HCO' AND M.attribute = 'pty_stat'
    LEFT JOIN {catalog}.bronze.survivorship_table N ON UPPER(A.src_nm) = UPPER(N.source) AND N.entity = 'HCO' AND N.attribute = 'pty_strsn_cd'
    WHERE A.ety_type = 'HCO'
),
FirstNonNull AS (
    SELECT DISTINCT
        cs_id,
        FIRST_VALUE(tgt_accnt_desc_4) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_tgt_accnt_desc_4) AS tgt_accnt_desc_4,
        FIRST_VALUE(tgt_accnt_desc_3) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_tgt_accnt_desc_3) AS tgt_accnt_desc_3,
        FIRST_VALUE(tgt_accnt_desc_2) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_tgt_accnt_desc_2) AS tgt_accnt_desc_2,
        FIRST_VALUE(tgt_accnt_desc_1) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_tgt_accnt_desc_1) AS tgt_accnt_desc_1,
        FIRST_VALUE(stan_full_nm) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_full_nm) AS stan_full_nm,
        FIRST_VALUE(kaiser_flg) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_kaiser_flg) AS kaiser_flg,
        FIRST_VALUE(stan_pty_status) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_pty_status) AS stan_pty_status,
        FIRST_VALUE(pty_strsn_cd) IGNORE NULLS OVER (PARTITION BY cs_id ORDER BY rank_pty_strsn_cd) AS pty_strsn_cd,
        ROW_NUMBER() OVER (
            PARTITION BY cs_id 
            ORDER BY 
                (CASE WHEN tgt_accnt_desc_4 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN tgt_accnt_desc_3 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN tgt_accnt_desc_2 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN tgt_accnt_desc_1 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN stan_full_nm IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN kaiser_flg IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN stan_pty_status IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN pty_strsn_cd IS NOT NULL THEN 1 ELSE 0 END) DESC
        ) AS row_num
    FROM RankedData
)
SELECT DISTINCT
    cs_id,
    tgt_accnt_desc_4 as ety_clas_cd_desc,
    tgt_accnt_desc_3 as ety_clas_type_cd_desc,
    tgt_accnt_desc_2 as ety_type_cd_desc,
    tgt_accnt_desc_1 as ety_sub_type_cd_desc,
    'HCO' AS ety_super_clas,
    stan_full_nm as full_name,
    kaiser_flg,
    stan_pty_status as pty_status,
    pty_strsn_cd
FROM FirstNonNull
WHERE row_num = 1
ORDER BY cs_id
""")

pty_hco_survived_df.createOrReplaceTempView("pty_hco_survived_view")

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
            TRIM(CONCAT(A.src_id, A.src_cd)) = TRIM(CONCAT(B.src_id, B.src_cd))
        WHERE 
            A.ety_type = 'HCO'
        AND
            COALESCE(prim_addr_flg, 'N') = 'N'
""").createOrReplaceTempView("unchanged_hco_address_survived_view")

# COMMAND ----------

spark.sql(f"""SELECT * FROM {catalog}.silver.premdm_pty_address WHERE coalesce(prim_addr_flg,'Y') = 'Y' and entity_type = 'HCO'""").createOrReplaceTempView("changed_hco_address_survived_view")

# COMMAND ----------

# Matching HCO table based on survivorship table
hco_address_survived_df = spark.sql(f"""
WITH RankedAddresses AS (
    SELECT DISTINCT
        B.cs_id,
        A.cs_id AS unique_src_id,
        A.src_nm,
        A.addr_id,
        A.stan_addr_line1,
        A.addr_line2,
        A.stan_city,
        A.stan_state,
        A.stan_zip5,
        A.prim_addr_flg,
        A.zip4,
        A.addr_type,
        A.stan_address_status,
        S.rank AS src_rank,
        ROW_NUMBER() OVER (
            PARTITION BY B.cs_id
            ORDER BY
                S.rank ASC,                       -- 1st priority: Source Rank (lower is better)
                LENGTH(A.stan_addr_line1) DESC,  -- 2nd priority: Longer address preferred
                A.cs_id ASC               -- 3rd priority: Lowest cs_id as tiebreaker
        ) AS rn
    FROM changed_hco_address_survived_view A
    INNER JOIN {catalog}.gold.pub_pty_xref B
        ON A.cs_id = B.unique_src_id
    LEFT JOIN (
        SELECT *
        FROM {catalog}.bronze.survivorship_table
        WHERE entity = 'HCO' AND attribute = 'prim_addr_flg'
    ) S
        ON UPPER(TRIM(A.src_nm)) = UPPER(TRIM(S.source))
    WHERE B.ety_type = 'HCO'
)

SELECT DISTINCT
    cs_id,
    addr_id,
    stan_address_status,
    stan_addr_line1,
    addr_line2,
    stan_city,
    stan_state,
    stan_zip5,
    prim_addr_flg,
    zip4,
    addr_type
FROM RankedAddresses
WHERE rn = 1
ORDER BY cs_id
""")

hco_address_survived_df.createOrReplaceTempView("hco_address_survived_view")



# COMMAND ----------

spark.sql(f"""
    SELECT
        UPPER(TRIM(cs_id)) as cs_id,
        UPPER(TRIM(addr_id)) as addr_id,
        UPPER(TRIM(stan_address_status)) as address_status,
        UPPER(TRIM(stan_addr_line1)) as address_line_1,
        CASE WHEN LENGTH(TRIM(addr_line2)) = 0 THEN NULL ELSE UPPER(TRIM(addr_line2)) END as address_line2,
        UPPER(TRIM(stan_city)) as city,
        UPPER(TRIM(stan_state)) as state,
        UPPER(TRIM(zip4)) as zip4,
        CASE WHEN LENGTH(UPPER(TRIM(stan_zip5))) <> 5 THEN NULL
        ELSE UPPER(TRIM(stan_zip5)) end as zip5,
        UPPER(TRIM(prim_addr_flg)) as primary_address,
        UPPER(TRIM(addr_type)) as address_type
    FROM
        hco_address_survived_view 
    
    UNION

    SELECT
        UPPER(TRIM(cs_id)) as cs_id,
        UPPER(TRIM(addr_id)) as addr_id,
        UPPER(TRIM(address_status)) as address_status,
        UPPER(TRIM(addr_line1)) as address_line_1,
        CASE WHEN LENGTH(TRIM(addr_line2)) = 0 THEN NULL ELSE UPPER(TRIM(addr_line2)) END as address_line2,
        UPPER(TRIM(city)) as city,
        UPPER(TRIM(state)) as state,
        UPPER(TRIM(zip4)) as zip4,
        CASE WHEN LENGTH(UPPER(TRIM(zip5))) <> 5 THEN NULL
        ELSE UPPER(TRIM(zip5)) end as zip5,
        UPPER(TRIM(prim_addr_flg)) as primary_address,
        UPPER(TRIM(addr_type)) as address_type
    FROM
        unchanged_hco_address_survived_view 
            
 """).createOrReplaceTempView("hco_address_survived_view")

# COMMAND ----------

pub_pty_hco_df = spark.sql(f"""
SELECT
    DISTINCT
    pty.cs_id as cs_id,
    pty.ety_clas_cd_desc ,
    pty.ety_clas_type_cd_desc ,
    pty.ety_sub_type_cd_desc ,
    pty.ety_super_clas ,
    pty.ety_type_cd_desc ,
    pty.full_name ,
    pty.kaiser_flg ,
    pty.pty_status ,
    pty_addr.addr_id ,
    pty_addr.address_status ,
    pty_addr.address_line_1 ,
    pty_addr.address_line2 ,
    pty_addr.city ,
    pty_addr.state ,
    pty_addr.zip4 ,
    pty_addr.zip5 ,
    pty_addr.primary_address ,
    pty_addr.address_type,
    DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS insrt_dt
    FROM
    pty_hco_survived_view pty
    LEFT JOIN
    hco_address_survived_view pty_addr ON pty.cs_id = pty_addr.cs_id 

""")
pub_pty_hco_df.createOrReplaceTempView("pub_pty_hco_view")
# pub_pty_hco_df.display()


# COMMAND ----------

pub_pty_hco_df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.pub_pty_hco")
