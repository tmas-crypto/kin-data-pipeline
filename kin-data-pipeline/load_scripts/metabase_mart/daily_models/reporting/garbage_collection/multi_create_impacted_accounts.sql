{create_or_insert}
    -- Step 1 - Identify transactions that match our established bad actor pattern
    abnormal_creates as (
        select
            transaction_id,
            --date_key,
            count(instr_idx) as instruction_count
        from {target_database}.kin_main_warehouse.fact_kin_creations
        group by transaction_id
        having instruction_count > 1
    ),
    -- Step 2 - Get the accounts associated to the identified transactions
    accounts_for_impacted_txns as (
        select
            distinct account_id as account_id
        from {target_database}.kin_main_warehouse.fact_kin_creations
        where transaction_id in (select transaction_id from abnormal_creates)
        -- We're only interested in accounts where the fee_payer is Agora
        -- As a Kinetic solution, will extend this to all fee_payers
        and fee_payer = 'agsWhfJ5PPGjmzMieWY8BR5o1XRVszUBQ5uFz4CtDiJ'
    ),
    -- Step 3 - Identify all accounts with a 0 transaction as candidates for closure
    account_balances_check as (
        select
            wallet_id,
            date_key,
            dense_rank() over (partition by wallet_id order by date_key desc) as balance_rank
        from {target_database}.kre_mart.closing_account_balance_per_day
        where wallet_id in (select account_id from accounts_for_impacted_txns)
        and closing_balance <= 0
        -- So we don't pull in accounts after they've zero'd out in future runs
        {incremental_filter}
    ),
    -- Step 4 - Get the account owner of impacted accounts that can be closed
    account_owners as (
        select
            -- date_key is the date the account is pulled into this model
            current_date as date_key,
            account_id,
            account_owner
        from {target_database}.kin_main_warehouse.fact_kin_creations
        where account_id in (select wallet_id from account_balances_check where balance_rank = 1)
    )
    select * from account_owners
{query_close}
