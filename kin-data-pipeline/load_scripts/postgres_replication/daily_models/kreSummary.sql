-- Re-naming columns to expected convention for API
select
    date_key as "date",
    active_apps as "activeApps",
    active_user_balance as "activeUserBalance",
    active_capped_user_balance as "activeCappedUserBalance",
    active_users as "activeUsers",
    volatility_factor as "dailyVolatilityFactor",
    kin_payout as "dailyKinPayout",
    usd_payout as "dailyUsdPayout",
    transaction_count as "dailyTransactions",
    monthly_active_earners as "monthlyActiveEarners",
    monthly_active_spenders as "monthlyActiveSpenders",
    monthly_active_users as "monthlyActiveUsers"
from {source_database}.kin_data.daily_kre_summary
{incremental_filter}
order by date_key asc
