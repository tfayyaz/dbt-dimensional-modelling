-- FILEPATH: /Users/tahir.fayyaz/databricks-dev/pm-technical-dev/dbt-dimensional-modelling/adventureworks/models/marts/rory.sql

-- BEGIN: Join dim_credit_card and fct_sales to get sales per cc type
{% set schema = target.schema %}
{% set model_name = "sales_per_cc_type" %}

{{ config(materialized='table', schema=schema, unique_key='cc_type') }}

SELECT
    cc.cc_type,
    SUM(sales.amount) AS total_sales,
    COUNT(sales.sales_key) AS total_transactions
FROM
    {{ ref('dim_credit_card') }} AS cc
JOIN
    {{ ref('fct_sales') }} AS sales ON cc.creditcard_key = sales.creditcard_key
GROUP BY
    cc.cc_type
-- END: Join dim_credit_card and fct_sales to get sales per cc type
