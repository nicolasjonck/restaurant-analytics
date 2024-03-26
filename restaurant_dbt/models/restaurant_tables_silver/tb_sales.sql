/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

SELECT
  id_order,
  id_store,
  id_table,
  id_waiter,
  CAST(date_opened AS TIMESTAMP) AS order_date_opened,
  CAST(date_closed AS TIMESTAMP) AS order_date_closed,
  dim_status,
  REGEXP_REPLACE(lower(dim_source), r'[^a-zA-Z0-9 -]', '') AS dim_source,
  CAST(m_nb_customer AS INTEGER) AS number_customer,
  CAST(m_cached_payed AS NUMERIC) AS cached_payed,
  CAST(m_cached_price AS NUMERIC) AS cached_price,

FROM {{ source ('restaurant_raw_data','orders')}}
WHERE id_order IS NOT null
AND id_store IS NOT null

QUALIFY ROW_NUMBER() OVER(PARTITION BY  id_order, id_store ORDER BY  order_date_closed DESC ) = 1

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
