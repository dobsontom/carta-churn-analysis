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
         contract_value,
         subscription_start_dttm,
         subscription_end_dttm,
         CASE
            WHEN DATE_ADD(subscription_end_dttm, INTERVAL 1 MONTH) < CURRENT_DATE THEN 1
            ELSE 0
         END AS churn_flag,
         `date`
         -- etc.
      FROM
         {{ ref('subscription_fact') }}
         JOIN start_date ON 1 = 1
      WHERE
         `date` >= start_date.start_date
   ),
   add_customer AS (
      SELECT
         s.*,
         c.customer_name,
         c.customer_email,
         c.customer_phone
         -- etc.
      FROM
         subscription_base s
         LEFT JOIN {{ ref('customer_dim') }} c ON s.location_id = c.location_id
   ),
   add_location AS (
      SELECT
         c.*,
         l.country,
         l.region,
         l.city
         -- etc.
      FROM
         add_customer c
         LEFT JOIN {{ ref('location_dim') }} l ON c.location_id = l.location_id
   )
SELECT
   *
FROM
   add_location;