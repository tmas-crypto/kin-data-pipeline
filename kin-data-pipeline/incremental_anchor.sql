incremental_date as (
    -- We never need anything prior to this date with Kins migration to Sol
    select coalesce(max(date_key), '2017-01-01') as last_insert_date, '2017-01-01'
    from {target_database}.{target_schema}.{target_model} 
),
