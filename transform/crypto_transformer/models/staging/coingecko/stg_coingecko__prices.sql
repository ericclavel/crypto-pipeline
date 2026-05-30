with raw_source as (
    select * from {{ source('coingecko', 'crypto_prices') }}
)

select
    id as price_id,
    coin_id as asset_id,
    price_usd,
    last_updated_at as recorded_at_utc,
    ingested_at as ingested_at_utc
from raw_source
