
{{ config(materialized='incremental', unique_key='group_name') }}

with raw as (
    select distinct group_name, "year"
    from fin_proj.t5_raw_ukrstat_trade_dict
    where group_name is not null
      and name_complex_eng is not null
),

ranked as (
    select
        group_name,
        "year",
        row_number() over (order by group_name) as id_group
    from raw
)

select *
from ranked

{% if is_incremental() %}
where "year" not in (select distinct "year" from {{ this }})
{% endif %}