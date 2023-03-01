-- Re-naming columns to expected convention for API
select
    date_key as "date",
    app_count as "monthlyActiveApps",
    transaction_fees_sol as "transactionFees",
    transactions_fee_usd as "transactionFeesUsd",
    transaction_count as "totalDailyTransactions",
    transaction_amount_kin as "totalDailyAmount",
    transaction_amount_usd as "totalDailyAmountUsd",
    earn_count as "dailyEarnTransactions",
    earn_amount_kin as "dailyEarnAmount",
    earn_amount_usd as "dailyEarnAmountUsd",
    spend_count as "dailySpendTransactions",
    spend_amount_kin as "dailySpendAmount",
    spend_amount_usd as "dailySpendAmountUsd",
    peer_count as "dailyPeerTransactions",
    peer_amount_kin as "dailyPeerAmount",
    peer_amount_usd as "dailyPeerAmountUsd",
    daily_active_users as "dailyActiveUsers",
    daily_active_earners as "dailyActiveEarners",
    daily_active_spenders as "dailyActiveSpenders",
    monthly_active_users as "monthlyActiveUsers",
    monthly_active_earners as "monthlyActiveEarners",
    monthly_active_spenders as "monthlyActiveSpenders"
from {source_database}.kin_data.daily_summary_ecosystem
{incremental_filter}
order by date_key asc
