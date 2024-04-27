WITH RankedPayments AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id_order ORDER BY m_amount DESC) as rank
  FROM {{ source ('restaurant_raw_data', 'payments')}}
), order_tips AS (
  SELECT
    s.id_order,
    IF(p.m_amount - order_price > 0, p.m_amount - order_price, 0) AS order_tip,
    SAFE_DIVIDE(IF(p.m_amount - order_price > 0, p.m_amount - order_price, 0), order_price) AS order_tip_percentage
  FROM
    {{ ref('tb_sales') }} s
  JOIN
    RankedPayments p
  ON s.id_order = p.id_order AND p.rank = 1
)
SELECT
  id_waiter,
  CASE id_waiter
    WHEN 7186 THEN 'Priya'
    WHEN 7187 THEN 'Yara'
    WHEN 7185 THEN 'Ivan'
    WHEN 7225 THEN 'Charlotte'
    WHEN 7405 THEN 'Ethan'
    WHEN 6643 THEN 'Arjun'
    WHEN 6607 THEN 'Anika'
    WHEN 9137 THEN 'Aisha'
    WHEN 9270 THEN 'Sakura'
    WHEN 603 THEN 'Ava'
    WHEN 465 THEN 'Fatima'
    WHEN 461 THEN 'Youssef'
    WHEN 16823 THEN 'Kai'
    WHEN 26319 THEN 'Santiago'
    WHEN 17650 THEN 'Nikolai'
    WHEN 13318 THEN 'Mei'
    WHEN 6693 THEN 'Jin'
    WHEN 2539 THEN 'Olivia'
    WHEN 24920 THEN 'Emma'
    WHEN 15509 THEN 'Raj'
    WHEN 24919 THEN 'Giulia'
    WHEN 24921 THEN 'Anaya'
    WHEN 25250 THEN 'Amelia'
    WHEN 10990 THEN 'Luca'
    WHEN 10836 THEN 'Mia'
    WHEN 10991 THEN 'Anastasia'
    WHEN 20061 THEN 'Muhammad'
    WHEN 10628 THEN 'Lena'
    WHEN 10627 THEN 'Haruto'
    WHEN 9665 THEN 'Isabella'
    WHEN 14172 THEN 'Oliver'
    WHEN 22355 THEN 'Logan'
    WHEN 9289 THEN 'Amina'
    WHEN 10279 THEN 'Chen'
    WHEN 12873 THEN 'Ali'
    WHEN 21061 THEN 'Noah'
    WHEN 17052 THEN 'Aryan'
    WHEN 24833 THEN 'Sophia'
    WHEN 14302 THEN 'Zahra'
    WHEN 25560 THEN 'Elena'
    WHEN 12894 THEN 'Mateo'
    WHEN 12895 THEN 'Camila'
    WHEN 12893 THEN 'Elijah'
    WHEN 15756 THEN 'Alejandro'
    WHEN 27245 THEN 'Liam'
    WHEN 8257 THEN 'Lucas'
    WHEN 8303 THEN 'Carmen'
    WHEN 1095 THEN 'Oluwatobi'
    WHEN 972 THEN 'Hye'
    WHEN 10411 THEN 'Leila'
    WHEN 25608 THEN 'Takashi'
    WHEN 2464 THEN 'Marisol'
    WHEN 2034 THEN 'Dmitri'
    WHEN 26272 THEN 'Nia'
    WHEN 10977 THEN 'Khaled'
    WHEN 10976 THEN 'Chika'
    WHEN 11211 THEN 'Lars'
    WHEN 2752 THEN 'Simone'
    WHEN 2753 THEN 'Farid'
  END as waiter_name,
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
  id_store,
  COUNT(DISTINCT CONCAT(id_table, id_order)) AS nb_tables,
  SUM(number_customer)/10 AS nb_customers_tst,
  COUNT(id_order)/10 AS nb_orders_tst,
  ROUND(AVG(TIMESTAMP_DIFF(order_date_closed , order_date_opened , minute))) AS avg_order_duration_min,
  ROUND(SUM(order_tip)) AS total_tips,
  ROUND(AVG(order_tip), 2) AS avg_tip,
  SUM(item_price)/10 as waiter_sales_tst
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
