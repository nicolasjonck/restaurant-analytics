/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


SELECT
  od.id_order_line as item_id,
  od.dim_name_translated as item_name,
  od.dim_category_translated as item_category,
  od.m_unit_price as item_price,
  od.m_quantity as item_quantity,
  o.id_order,
  o.id_store,
  o.id_table,
  o.id_waiter,
  CAST(o.date_opened AS TIMESTAMP) AS order_date_opened,
  CAST(o.date_closed AS TIMESTAMP) AS order_date_closed,
  o.dim_status,
  CAST(o.m_cached_price AS NUMERIC) AS order_price,
  CAST(o.m_cached_payed AS NUMERIC) AS payed_price,
  CAST(o.m_nb_customer AS INTEGER) AS number_customer
 
FROM {{ source ('restaurant_raw_data','orders')}} AS o
INNER JOIN {{ source ('restaurant_raw_data','order_details')}} AS od
USING (id_order)
WHERE o.id_order IS NOT NULL
AND o.id_store IS NOT NULL
AND od.m_unit_price > 0

QUALIFY ROW_NUMBER() OVER(PARTITION BY  id_order, id_store ORDER BY  order_date_closed DESC ) = 1

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
