/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

WITH DeduplicatedOrders AS (
  SELECT id_order,
    id_store,
    id_table,
    id_waiter,
    CAST(date_opened AS TIMESTAMP) AS order_date_opened,
    CAST(date_closed AS TIMESTAMP) AS order_date_closed,
    dim_status,
    CAST(m_cached_price AS NUMERIC) AS order_price,
    CAST(m_cached_payed AS NUMERIC) AS payed_price,
    CAST(m_nb_customer AS INTEGER) AS number_customer
  FROM {{ source ('restaurant_raw_data','orders')}}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY  id_order, id_store ORDER BY  order_date_closed DESC ) = 1
)

SELECT
  od.id_order_line as item_id,
  od.dim_name_translated as item_name,
  od.dim_category_translated as item_category,
  od.m_total_price_inc_vat as item_price,
  od.m_quantity as item_quantity,
  o.id_order,
  o.id_store,
  o.id_table,
  o.id_waiter,
  o.order_date_opened,
  o.order_date_closed,
  o.dim_status,
  o.order_price,
  o.payed_price,
  o.number_customer
FROM DeduplicatedOrders AS o
INNER JOIN {{ source ('restaurant_raw_data','order_details')}} AS od
USING (id_order)
WHERE o.id_order IS NOT NULL
AND o.id_store IS NOT NULL
AND od.m_unit_price > 0


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
