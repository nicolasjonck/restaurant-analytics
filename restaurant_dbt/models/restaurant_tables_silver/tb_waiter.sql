WITH order_tips AS (
  SELECT
    id_order,
    IF(p.m_amount - order_price > 0, p.m_amount - order_price, 0) AS order_tip,
    SAFE_DIVIDE(IF(p.m_amount - order_price > 0, p.m_amount - order_price, 0), order_price) AS order_tip_percentage
  FROM
    {{ ref('tb_sales') }}
  JOIN
    {{ source ('restaurant_raw_data', 'payments')}} p
  USING(id_order)
)
SELECT
  id_waiter,
  id_store,
  COUNT(DISTINCT CONCAT(id_table, id_order)) AS nb_tables,
  SUM(number_customer) AS nb_customers,
  COUNT(id_order) AS nb_orders,
  ROUND(AVG(TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute))) AS avg_order_duration_min,
  ROUND(SUM(order_tip)) AS total_tips,
  ROUND(AVG(order_tip), 2) AS avg_tip,
  SUM(order_price) as total_sales
FROM
  {{ ref('tb_sales') }}
JOIN
  order_tips o
USING(id_order)
WHERE
  id_waiter IS NOT NULL AND id_waiter != 0
GROUP BY
  id_waiter, id_store
-- filter out outliers
HAVING
  AVG(order_tip) < 15
