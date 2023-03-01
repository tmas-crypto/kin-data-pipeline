-- Re-naming columns to expected convention for API
select
    date_key as "date",
    kin_price as "price",
    kin_market_cap as "marketCap",
    kin_trade_volume as "tradeVolume",
    kin_circulating_supply "circulatingSupply"
from {source_database}.kin_data.daily_market_summary
{incremental_filter}
order by date_key asc
