{{
  config(
    materialized='table'
  )
}}

WITH
   -- Limit the volume of data being processed to the date range agreed with the Sales Director.
   -- This filter could be applied at the staging phase for improved performance and cost savings,
   -- but it is added here for clarity in the final model.
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
         -- Apply churn flag logic with a one week buffer as agreed with the Sales Director.
         -- This could be done earlier in the pipeline for efficiency, but it is included here
         -- for visibility in this example.
         CASE
            WHEN DATE_ADD(subscription_end_dttm, INTERVAL 1 WEEK) < CURRENT_DATE THEN 1
            ELSE 0
         END AS churn_flag,
         `date`
         -- etc.
      FROM
         {{ ref('subscription_fact') }}
         -- An explicit join is used here for ease of reading and maintainability.
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
         LEFT JOIN {{ ref('customer_dim') }} c ON s.customer_id = c.customer_id
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
   
-- Final selection of data, integrating subscription, customer, and location details.
SELECT
   *
FROM
   add_location;
