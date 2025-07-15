-- курс валют - медіана по місяцю + median_rate_change_ratio 
--(динаміка курсу: чи гривня зміцнилась чи ослабла в порівнянні з попереднім місяцем)
{{ config(
    materialized = 'incremental',
    unique_key = ['currency_code', 'month_date']
) }}

with monthly_median as (
    select  
        currency_code,
        currency_name,
        date_trunc('month', date) as month_date,
        to_char(date_trunc('month', date), 'YYYY-MM-DD') as mon_01date,
        to_char(date, 'YYYY-MM') as period,
        percentile_cont(0.5) within group (order by rate) as median_rate_to_UAH
    from fin_proj.t1_raw_exch_rates
    group by currency_code, currency_name, date_trunc('month', date), to_char(date, 'YYYY-MM')
)

select
    *,
    median_rate_to_UAH / lag(median_rate_to_UAH) over (
        partition by currency_code
        order by period
    ) as median_rate_change_ratio

from monthly_median

{% if is_incremental() %}
  where date_trunc('month', month_date) not in (
    select date_trunc('month', month_date) from {{ this }}
  )
{% endif %}