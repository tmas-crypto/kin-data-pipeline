-- Re-naming columns to expected convention for API
select 
    date_key as "date",
    accounts_created as "accountsCreated",
    sol_cost as "solCost",
    usd_cost as "usdCost"
from {source_database}.kin_data.daily_account_summary_ecosystem
{incremental_filter}
order by date_key asc
