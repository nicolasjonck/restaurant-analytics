SELECT
  id_item,
  item_name,
  item_category,
  item_price,
  item_quantity,
  id_order,
  id_store,
  id_table,
  COALESCE(id_waiter, 000) AS id_waiter,
  order_date_opened,
  order_date_closed,
  TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute) as order_duration,
  dim_status,
  order_price,
  IF(p.m_amount - s.order_price > 0, p.m_amount - s.order_price, 0) AS order_tip,
  IF(p.m_amount - s.order_price > 0, p.m_amount - s.order_price, 0)/s.order_price as order_tip_percentage,
  IF(p.m_amount - s.order_price < 0, s.order_price - p.m_amount, 0) AS voucher_amount,
  IF(p.m_amount - s.order_price < 0, s.order_price - p.m_amount, 0)/s.order_price as voucher_percentage,
  number_customer
  IF(TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute) < 5, "take away", "dine in") AS dinner_type

FROM {{ source ('restaurant_silver_data','tb_sales')}} AS s
JOIN {{ source ('restaurant_raw_data', 'payments')}} AS p
ON
  s.id_order = p.id_order
Where s.order_price > 0;


