-- Re-naming columns to expected convention for API
select 
    date_key as "date",
    kin_payout as "kin",
    usd_payout as "usd",
    top_app_list as "top10"
from {source_database}.kin_data.weekly_kre_payout_summary
{incremental_filter}
order by date_key asc
