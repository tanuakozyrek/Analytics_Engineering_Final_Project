{{ config(materialized='incremental', unique_key='subgroup') }}

with raw as (
    select distinct
        group_name,
       translate(
  regexp_replace(
    lower(trim(regexp_replace(name_complex, '[\t\r\n ]+', ' ', 'g'))),
    '[’‘´`"«»]', '''', 'g'
  ),
  'iIeEoOaA',
  'іІеЕоОаА'
) as subgroup,
        trim(name_complex_eng) as subgroup_eng,
lpad(regexp_replace(left(name_complex, 2), '\s', '', 'g'), 2, '0') as product_id,
        "year"
    from fin_proj.t5_raw_ukrstat_trade_dict
    where group_name is not null
      and name_complex_eng is not null
),

group_ids as (
    select group_name, id_group, year
    from {{ ref('stg_ukrstat_trade_dict_group') }}
),

joined as (
    select
        r.subgroup,
        r.subgroup_eng,
        r.product_id,
        g.id_group,
        r."year"
    from raw r
    join group_ids g
      on r.group_name = g.group_name and r."year" = g.year
)

select *
from joined

{% if is_incremental() %}
where "year" not in (select distinct "year" from {{ this }})
{% endif %}