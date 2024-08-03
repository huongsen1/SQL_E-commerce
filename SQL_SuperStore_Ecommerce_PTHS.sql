-- bigquery: ga_session

----------Query 01: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (ORDER BY month)

SELECT 
  FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS views,
  SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY transactions;


----------Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (ORDER BY total_visit DESC)

SELECT 
  trafficSource.source,
  SUM(totals.visits) AS total_visits,
  SUM(totals.bounces) AS total_no_of_bounces,
  SUM(totals.bounces)/SUM(totals.visits)*100 AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;


----------Query 3: Revenue by traffic source by week, by month in June 2017

SELECT *
FROM(
  SELECT
  	'Month' AS time_type,
  	FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS time,
  	trafficSource.source,
  	SUM(product.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE product.productRevenue is not null
  GROUP BY source,time
  ORDER BY source, time, revenue DESC
  )
  AS t1

UNION ALL

SELECT 
  'Week' AS time_type,
  CONCAT(EXTRACT(year FROM (parse_date('%Y%m%d',date))),EXTRACT(week FROM (parse_date('%Y%m%d',date)))) AS time,
  trafficSource.source,
  SUM(product.productRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
WHERE product.productRevenue is not null
GROUP BY source,time
ORDER BY source, time, revenue DESC;


----------Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

WITH purchase AS(
  SELECT 
    FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
    SUM(totals.pageviews)/count(distinct fullVisitorId) AS avg_pageviews_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
    AND totals.transactions >= 1
    AND product.productRevenue is not null
  GROUP BY month),
nonpurchase AS(
  SELECT 
    FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
    SUM(totals.pageviews)/count(distinct fullVisitorId) AS avg_pageviews_non_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
    AND totals.transactions is null
    AND product.productRevenue is null
  GROUP BY month)

SELECT 
  p.month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
FROM purchase p
LEFT JOIN nonpurchase np
ON p.month = np.month
ORDER BY p.month;


----------Query 05: Average number of transactions per user that made a purchase in July 2017

SELECT 
  FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
  SUM(totals.transactions)/count(distinct fullVisitorId) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
WHERE totals.transactions >= 1
  AND product.productRevenue is not null
GROUP BY month;


----------Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
				-- avg_spend_per_session = total revenue/ total visit
SELECT 
  FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
  ROUND(
  	(SUM(product.productRevenue)/1000000)
  	/SUM(totals.visits)
  	,2) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
WHERE totals.transactions is not null
  AND product.productRevenue is not null
GROUP BY month;


----------Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. 
				-- Output should show product name and the quantity was ordered.
SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
WHERE product.productQuantity is not null
  AND product.productRevenue is not null
  AND fullVisitorId in ( 
                      SELECT 
                        fullVisitorId
                      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                      UNNEST(hits) hits,
                      UNNEST(hits.product) product
                      WHERE product.productRevenue is not null
                        AND product.v2ProductName = "YouTube Men's Vintage Henley")
  AND product.v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY product.v2ProductName
ORDER BY quantity DESC;


----------Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. 
				-- For example, 100% product view then 40% add_to_cart and 10% purchase.
				-- Add_to_cart_rate = number product  add to cart/number product view. 
				-- Purchase_rate = number product purchase/number product view.
				-- The output should be calculated in product level. 

--Cách 1:dùng CTE
WITH product_view AS(
  SELECT 
    FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
    count(eCommerceAction.action_type) AS num_product_view
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product   
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '2' 
  GROUP BY month
),

add_to_cart AS(
  SELECT 
    FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
    count(eCommerceAction.action_type) AS num_addtocart
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product   
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '3' 
  GROUP BY month
),

purchase AS(
  SELECT 
    FORMAT_DATE('%Y%m',parse_date('%Y%m%d',date)) AS month,
    count(eCommerceAction.action_type) AS num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product   
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '6'
    AND product.productRevenue is not null
  GROUP BY month
)

SELECT
    pv.*,
    num_addtocart,
    num_purchase,
    ROUND(num_addtocart*100/num_product_view,2) AS add_to_cart_rate,
    ROUND(num_purchase*100/num_product_view,2) AS purchase_rate
from product_view pv
LEFT JOIN add_to_cart a ON pv.month = a.month
LEFT JOIN purchase p ON pv.month = p.month
ORDER BY pv.month;

--Cách 2: Dùng count(case when) 
WITH product_data AS(
SELECT
    FORMAT_DATE('%Y%m', parse_date('%Y%m%d',date)) AS month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) AS num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) AS num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' AND product.productRevenue is not null THEN product.v2ProductName END) AS num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST(hits) AS hits,
UNNEST (hits.product) AS product
WHERE _table_suffix BETWEEN '0101' AND '0331'
AND eCommerceAction.action_type in ('2','3','6')
GROUP BY month
ORDER BY month
)

SELECT
    *,
    ROUND(num_add_to_cart/num_product_view * 100, 2) AS add_to_cart_rate,
    ROUND(num_purchase/num_product_view * 100, 2) AS purchase_rate
FROM product_data;


--The end--
