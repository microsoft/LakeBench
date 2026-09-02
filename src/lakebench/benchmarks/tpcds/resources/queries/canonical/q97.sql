WITH ssci AS (
  SELECT
    ss_customer_sk AS customer_sk,
    ss_item_sk AS item_sk
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE
    d_month_seq BETWEEN 1183 AND 1183 + 11
  GROUP BY
    ss_customer_sk,
    ss_item_sk
), csci AS (
  SELECT
    cs_bill_customer_sk AS customer_sk,
    cs_item_sk AS item_sk
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE
    d_month_seq BETWEEN 1183 AND 1183 + 11
  GROUP BY
    cs_bill_customer_sk,
    cs_item_sk
)
SELECT
  SUM(
    CASE
      WHEN NOT ssci.customer_sk IS NULL AND csci.customer_sk IS NULL
      THEN CAST(1 AS BIGINT)
      ELSE CAST(0 AS BIGINT)
    END
  ) AS store_only,
  SUM(
    CASE
      WHEN ssci.customer_sk IS NULL AND NOT csci.customer_sk IS NULL
      THEN CAST(1 AS BIGINT)
      ELSE CAST(0 AS BIGINT)
    END
  ) AS catalog_only,
  SUM(
    CASE
      WHEN NOT ssci.customer_sk IS NULL AND NOT csci.customer_sk IS NULL
      THEN CAST(1 AS BIGINT)
      ELSE CAST(0 AS BIGINT)
    END
  ) AS store_and_catalog
FROM ssci
FULL OUTER JOIN csci
  ON (
    ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk
  )
LIMIT 100