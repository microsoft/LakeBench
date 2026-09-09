SELECT
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  SUM(ws_ext_sales_price) AS itemrevenue,
  SUM(ws_ext_sales_price) * 100 / NULLIF(
    SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class),
    0
  ) AS revenueratio
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE
  i_category IN ('Music', 'Sports', 'Children')
  AND d_date BETWEEN CAST('1998-03-14' AS DATE) AND DATE_ADD(CAST('1998-03-14' AS DATE), 30)
GROUP BY
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price
ORDER BY
  i_category,
  i_class,
  i_item_id,
  i_item_desc,
  revenueratio
LIMIT 100
