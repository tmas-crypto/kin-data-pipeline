{create_or_insert}
    from_accounts as (
        select
            date_key,
            app_id,
            from_account as account_id
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id, from_account
    ),
    to_accounts as (
        select
            date_key,
            app_id,
            to_account as account_id
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id, to_account
    ),
    accounts_union as (
        select * from from_accounts
        union
        select * from to_accounts
    ),
    distinct_accounts as (
        select 
            date_key,
            app_id,
            account_id
        from accounts_union
        group by date_key, app_id, account_id
    ),
    account_balances as (
        select
            account.date_key,
            account.app_id,
            account.account_id,
            bal.closing_balance
        from distinct_accounts account
        join {target_database}.kre_mart.closing_account_balance_per_day bal on account.date_key = bal.date_key 
            and account.account_id = bal.wallet_id
    ),
    account_balance_range_assignment as (
        select
            date_key,
            app_id,
            case
                when closing_balance >= 0 and closing_balance < 1 then 1
                when closing_balance >= 1 and closing_balance < 10 then 2
                when closing_balance >= 10 and closing_balance < 1000 then 3
                when closing_balance >= 1000 and closing_balance < 10000 then 4
                when closing_balance >= 10000 and closing_balance < 1000000 then 5
                when closing_balance >= 1000000 and closing_balance < 10000000 then 6
                when closing_balance >= 10000000 and closing_balance < 100000000 then 7
                when closing_balance >= 100000000  then 8
                else 0
            end as account_balance_range_bucket_id,
            case
                when closing_balance >= 0 and closing_balance < 1 then '0 - 1'
                when closing_balance >= 1 and closing_balance < 10 then '1 - 10' 
                when closing_balance >= 10 and closing_balance < 1000 then '10 - 1,000'
                when closing_balance >= 1000 and closing_balance < 10000 then '1,000 - 10,000'
                when closing_balance >= 10000 and closing_balance < 1000000 then '10,000 - 1,000,000'
                when closing_balance >= 1000000 and closing_balance < 10000000 then '1,000,000 - 10,000,000'
                when closing_balance >= 10000000 and closing_balance < 100000000 then '10,000,000 - 100,000,000'
                when closing_balance >= 100000000  then '100,000,000+'
                else 'Unknown'
            end as account_balance_range_bucket
        from account_balances
    )
    select
        date_key,
        app_id,
        account_balance_range_bucket_id,
        account_balance_range_bucket,
        count(*) as balance_range_count
    from account_balance_range_assignment
    group by date_key, app_id, account_balance_range_bucket_id, account_balance_range_bucket
{query_close}