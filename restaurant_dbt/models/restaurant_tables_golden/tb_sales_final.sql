WITH weather_data AS (
  SELECT DATE(date) as date_day,
    round(avg(temperature_2m), 2) as avg_temp,
    round(sum(rain), 2) as rain_cm,
    case
      when sum(snowfall) > 0.01 then 'snow'
      when sum(rain) > 0.01 AND avg(temperature_2m) < 10 then 'rain_very_cold'
      when sum(rain) > 0.01 AND avg(temperature_2m) < 15 then 'rain_cold'
      when sum(rain) > 0.01 AND avg(temperature_2m) < 20 then 'rain_warm'
      when sum(rain) > 0.01 AND avg(temperature_2m) > 20 then 'rain_hot'
      when avg(temperature_2m) < 10 then 'dry_very_cold'
      when avg(temperature_2m) < 15 then 'dry_cold'
      when avg(temperature_2m) < 20 then 'dry_warm'
      when avg(temperature_2m) > 20 then 'dry_hot'
      else null
    end as weather_type
  FROM {{ source ('weather_api_data', 'daily_temperature_rain')}}
  group by date_day
)

SELECT
  item_id,
  item_name,
  item_category,
  item_price,
  item_quantity,
  s.id_order,
  s.id_store,
  s.id_table,
  COALESCE(id_waiter, 000) AS id_waiter,
  order_date_opened,
  order_date_closed,
  TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute) as order_duration,
  s.dim_status,
  order_price,
  IF(p.m_amount - s.order_price > 0, p.m_amount - s.order_price, 0) AS order_tip,
  IF(p.m_amount - s.order_price > 0, p.m_amount - s.order_price, 0)/s.order_price as order_tip_percentage,
  IF(p.m_amount - s.order_price < 0, s.order_price - p.m_amount, 0) AS voucher_amount,
  IF(p.m_amount - s.order_price < 0, s.order_price - p.m_amount, 0)/s.order_price as voucher_percentage,
  number_customer,
  IF(TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute) < 5, "take away", "dine in") AS dinner_type,
  EXTRACT(DAYOFWEEK FROM order_date_closed) AS day_of_week,
  EXTRACT(HOUR FROM order_date_closed) AS hour_of_day,
  avg_temp,
  rain_cm,
  weather_type
FROM {{ source ('restaurant_silver_data','tb_sales')}} AS s
JOIN {{ source ('restaurant_raw_data', 'payments')}} AS p
ON
  s.id_order = p.id_order
LEFT JOIN weather_data AS w
ON DATE(s.order_date_closed) = DATE(w.date_day)
WHERE s.order_price > 0

QUALIFY ROW_NUMBER() OVER(PARTITION BY  id_order, id_store ORDER BY  order_date_closed DESC ) = 1
