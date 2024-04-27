{{ config(materialized='table') }}

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
), RankedPayments AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id_order ORDER BY m_amount DESC) as rank
  FROM {{ source ('restaurant_raw_data', 'payments')}}
)

SELECT
  item_id,
  CONCAT(UPPER(SUBSTR(item_name, 1, 1)), LOWER(SUBSTR(item_name, 2))) AS order_item,
  consolidated_category,
  item_category,
  item_price,
  item_quantity,
  s.id_order,
  s.id_store,
  CASE id_store
    WHEN 7872 THEN 'Super Shisha'
    WHEN 8291 THEN 'Eva Brunch'
    WHEN 7786 THEN 'Smooth Coffee'
    WHEN 7304 THEN 'Brasserie Jacques'
    WHEN 6830 THEN 'Punjab Tandoori'
    WHEN 6827 THEN 'Al Pronto'
    WHEN 5860 THEN 'Happy Duck'
    WHEN 5617 THEN 'The Crazy Horse'
    WHEN 5498 THEN 'Versatile'
    WHEN 5281 THEN 'The Coffee Bean'
    WHEN 5210 THEN 'Brasserie Maggy'
    WHEN 4803 THEN 'The Mellow Bowl'
    WHEN 4364 THEN 'Chez Hatem'
    WHEN 4196 THEN 'Seabird'
    WHEN 4151 THEN 'Beers & Pints'
    WHEN 2035 THEN 'Manneken Pils'
    WHEN 1513 THEN 'Kantipur Nepali'
    WHEN 730 THEN 'Taste of Curry'
    WHEN 360 THEN 'Bella Piazza'
    WHEN 351 THEN 'Afterwine'
  END as store_name,
  s.id_table,
  COALESCE(id_waiter, 000) AS id_waiter,
  DATE(order_date_closed) AS order_date,
  order_date_opened,
  order_date_closed,
  TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute) as order_duration,
  s.dim_status,
  order_price,
  AVG(order_price) OVER (
    PARTITION BY id_store
    ORDER BY order_date_closed
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS order_price_weekly_moving_avg,
  IF(p.m_amount - s.order_price > 0, p.m_amount - s.order_price, 0) AS order_tip,
  IF(p.m_amount - s.order_price > 0, p.m_amount - s.order_price, 0)/s.order_price as order_tip_percentage,
  IF(p.m_amount - s.order_price < 0, s.order_price - p.m_amount, 0) AS voucher_amount,
  IF(p.m_amount - s.order_price < 0, s.order_price - p.m_amount, 0)/s.order_price as voucher_percentage,
  number_customer,
  IF(TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute) < 2, "take away", "dine in") AS type_dinner,
  CONCAT(LEFT(FORMAT_DATE('%A', EXTRACT(DATE FROM order_date_closed)), 3), '.') AS day_of_week,
  EXTRACT(HOUR FROM order_date_closed) AS hour_of_day,
  avg_temp,
  rain_cm,
  weather_type
FROM {{ source ('restaurant_silver_data','tb_sales')}} AS s
JOIN RankedPayments AS p
ON
  s.id_order = p.id_order AND p.rank = 1
LEFT JOIN weather_data AS w
ON
  DATE(s.order_date_closed) = DATE(w.date_day)
JOIN {{ source ('restaurant_silver_data', 'tb_item_categories')}} AS c
ON
  s.item_category = c.original_category
WHERE s.order_price > 0
  AND LOWER(item_name) NOT LIKE '%deposit%'
  AND LOWER(item_name) NOT LIKE '%conso%'
  AND id_store != 6008
