{{ config(
    materialized = 'table'
) }}

with trade_data as (
    select * from {{ ref('stg_ukrstat_trade_data_country') }}
    where country not in ('ВСЬОГО:', 'ЕСВАТІНІ', 'НЕВИЗНАЧЕНІ КРАЇНИ')
),
country_id_map as (
    select * from {{ ref('country_dict_id') }}
),
country_dict as (
    select * from {{ ref('country_dict') }}
),
exchange_rates as (
    select * from {{ ref('stg_exch_rates_median_month') }}
    where currency_code = 'USD'
)

select
    t1.country,
    t1.year,
    t1.period,
    t1.date,
    t1.export_value_month,
    t1.import_value_month,
    t4.name_en,
    t5.country_id,
    t6.median_rate_to_UAH,
    t6.median_rate_change_ratio
from trade_data t1
left join country_id_map t5
    on t1.country = t5.name_for_stg
left join country_dict t4
    on t5.country_id = t4.country_id
left join exchange_rates t6
    on t1.date = to_date(t6.mon_01date, 'YYYY-MM-DD')
order by t1.country, t1.date