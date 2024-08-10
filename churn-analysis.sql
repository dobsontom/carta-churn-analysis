WITH
   start_date AS (
      SELECT
         DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEARS) AS start_date
   ),
   subscription_base AS (
      SELECT
         subscription_id,
         customer_id,
         location_id,
         subscription_start_dttm,
         subscription_end_dttm,
         active_status,
         DATE,
         etc
      FROM
         `data.warehouse.customer_fact`
         JOIN start_date ON 1 = 1
      WHERE
         DATE >= dates.start_date
   )
SELECT
   *
FROM
   subscription_base;

-- customer_name,
-- customer_email,
-- customer_phone,
-- contract_value