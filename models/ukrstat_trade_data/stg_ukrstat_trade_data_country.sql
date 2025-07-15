-- оновлення всіх даних з початку поточного року 
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
        and group_label is null
        and product_name is null
        and name_complex not in ('Різне')
),

transformed as (
    select 
        name_complex as country,
        export_value_th_dol_usa as export_value_th_dol_usa_runtotal,
        import_value_th_dol_usa as import_value_th_dol_usa_runtotal,
        "year",
        "period",
        "date",
        export_value_th_dol_usa - coalesce(
            lag(export_value_th_dol_usa) over (partition by name_complex, "year" order by period), 0
        ) as export_value_month,
        import_value_th_dol_usa - coalesce(
            lag(import_value_th_dol_usa) over (partition by name_complex, "year" order by period), 0
        ) as import_value_month
    from filtered
)

select * from transformed