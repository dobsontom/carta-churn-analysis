{{
  config(
    materialized='table'
  )
}}

WITH
   -- Used to limit the volume of data being processed to the agreed date range. This filter 
   -- could be applied at the staging phase for improved performance and cost savings,
   -- but is included here in the final model for clarity.
   start_date AS (
      SELECT
         DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEARS) AS start_date
   ),
   
   subscription_base AS (
      SELECT
         sf.subscription_id,
         sf.customer_id,
         sf.location_id,
         sf.contract_value,
         sf.subscription_start_dttm,
         sf.subscription_end_dttm,
         -- Apply churn flag logic with a one week buffer as agreed with the Sales Director.
         -- This could be done earlier in the pipeline for efficiency, but it is included in
         -- this example for visibility.
         CASE
            WHEN CURRENT_DATE > DATE_ADD(sf.subscription_end_dttm, INTERVAL 1 WEEK) THEN 1
            ELSE 0
         END AS churn_flag,
         row_date
         -- etc.
      FROM
         {{ ref('subscription_fact') }} sf
         -- An explicit join is used here for ease of reading and maintainability.
         JOIN start_date sd ON 1 = 1
      WHERE
         row_date >= sd.start_date
   ),

   add_customer AS (
      SELECT
         sb.*,
         cd.customer_name,
         cd.customer_email,
         cd.customer_phone
         -- etc.
      FROM
         subscription_base sb
         LEFT JOIN {{ ref('customer_dim') }} cd ON sb.customer_id = cd.customer_id
   ),

   add_location AS (
      SELECT
         c.*,
         ld.country,
         ld.region,
         ld.city
         -- etc.
      FROM
         add_customer c
         LEFT JOIN {{ ref('location_dim') }} ld ON c.location_id = ld.location_id
   )

   -- Final selection of data, integrating subscription, customer, and location details.
SELECT
   subscription_id,
   customer_id,
   location_id,
   contract_value,
   subscription_start_dttm,
   subscription_end_dttm,
   customer_name,
   customer_email,
   customer_phone,
   country,
   region,
   city,
   churn_flag
FROM
   add_location;
