WITH RankedItems AS (
  SELECT weather_type,
    order_item,
    consolidated_category,
    COUNT(*) as items_sold,
    store_name,
    ROW_NUMBER() OVER(PARTITION BY weather_type, store_name, consolidated_category ORDER BY COUNT(*) DESC) AS item_rank
    FROM {{ ref('tb_sales_final') }}
    GROUP BY weather_type, order_item, consolidated_category, store_name
)

SELECT weather_type,
  order_item,
  consolidated_category,
  items_sold,
  item_rank,
  store_name
FROM RankedItems
WHERE item_rank <= 5
ORDER BY weather_type, store_name
