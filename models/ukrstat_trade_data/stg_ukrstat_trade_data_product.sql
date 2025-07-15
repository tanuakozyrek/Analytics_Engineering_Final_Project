-- оновлення всіх даних по товарах з початку поточного року 
-- для забезпеч.врахування всіх змін в файлах за поточний рік
{{ config(materialized='table') }}

{% set current_year = modules.datetime.datetime.utcnow().year %}

with source as (
    select *
    from {{ source('fin_proj', 't4_raw_ukrstat_trade_data') }}
),

filtered as (
    select *
    from source
    where 
        "year" = {{ current_year }}
        and group_label is not null
        and product_name is not null
),

transformed as (
    select 
        group_label,
        translate(
  regexp_replace(
    lower(
      trim(
        regexp_replace(product_name, '[\t\r\n ]+', ' ', 'g')
      )
    ),
    '[’‘´`"«»]', '''', 'g'
  ),
  'iIeEoOaA',
  'іІеЕоОаА'
) as product_name,
lpad(regexp_replace(left(product_name, 2), '\s', '', 'g'), 2, '0') as product_id,
        export_value_th_dol_usa as export_value_th_dol_usa_runtotal,
        import_value_th_dol_usa as import_value_th_dol_usa_runtotal,
        "year",
        "period",
        "date",
        export_value_th_dol_usa - coalesce(
            lag(export_value_th_dol_usa) over (partition by group_label, product_name order by period), 0
        ) as export_value_month,
        import_value_th_dol_usa - coalesce(
            lag(import_value_th_dol_usa) over (partition by group_label, product_name order by period), 0
        ) as import_value_month
    from filtered
)

select * from transformed