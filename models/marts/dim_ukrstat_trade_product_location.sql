{{ config(
    materialized = 'incremental',
    unique_key = ['product_id', 'group_label', 'year', 'period']
) }}

with trade_data as (
    select * from {{ ref('stg_ukrstat_trade_data_product') }}
),
dict_subgroup as (
    select * from {{ ref('stg_ukrstat_trade_dict_subgr') }}
),
dict_group as (
    select * from {{ ref('stg_ukrstat_trade_dict_group') }}
),
country_id_map as (
    select * from {{ ref('country_dict_id') }}
),
country_dict as (
    select * from {{ ref('country_dict') }}
)

select
    t1.group_label,
    t1.product_name,
    t2.subgroup_eng,
    t1.product_id,
    t3.group_name,
    t2.id_group,
    t1.export_value_th_dol_usa_runtotal,
    t1.import_value_th_dol_usa_runtotal,
    t1.year,
    t1.period,
    t1.date,
    t1.export_value_month,
    t1.import_value_month,
    t4.name_en,
    t5.country_id

from trade_data t1
left join dict_subgroup t2
    on t1.product_id = t2.product_id and t1.year = t2.year
left join dict_group t3
    on t2.id_group = t3.id_group and t2.year = t3.year
left join country_id_map t5
    on t1.group_label = t5.name_for_stg
left join country_dict t4
    on t5.country_id = t4.country_id

where t1.group_label not in ('ВСЬОГО:', 'ЕСВАТІНІ', 'НЕВИЗНАЧЕНІ КРАЇНИ')

{% if is_incremental() %}
  and t1."year" = date_part('year', now())
{% endif %}