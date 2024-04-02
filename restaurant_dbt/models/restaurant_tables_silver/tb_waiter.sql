/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

SELECT
  id_waiter,
  SUM(IF(
    payed_price - order_price > 0,
    payed_price - order_price ,
    0
  )) AS sum_tip,

  SUM(IF(
    payed_price - order_price < 0,
    payed_price - order_price ,
    0
  )) AS sum_unpayed_order,

  COUNT(DISTINCT CONCAT(id_table, id_order)) AS nb_tables,

  AVG(TIMESTAMP_DIFF(order_date_opened , order_date_closed , hour)) AS avg_working_hours,

  COUNT(DISTINCT CASE WHEN dim_status LIKE 'CLOSED' THEN id_order END)AS closed_orders,
  COUNT(DISTINCT CASE WHEN dim_status NOT LIKE 'CLOSED' THEN id_order END)AS un_closed_orders,


FROM {{ ref ('tb_sales') }}
WHERE id_waiter IS NOT NULL
GROUP BY id_waiter


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
