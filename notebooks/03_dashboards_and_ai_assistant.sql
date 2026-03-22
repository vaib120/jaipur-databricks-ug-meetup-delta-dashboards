-- Databricks notebook source
USE CATALOG demo;
USE SCHEMA main;

-- COMMAND ----------

-- DBTITLE 1,Data Overview KPIs
SELECT
  COUNT(DISTINCT account_number)                            AS unique_accounts,
  COUNT(*)                                                  AS total_bills,
  MIN(bill_issue_date)                                      AS oldest_bill,
  MAX(bill_issue_date)                                      AS latest_bill,
  ROUND(AVG(consumption_units), 1)                          AS avg_units,
  ROUND(SUM(current_amount_due), 0)                         AS total_billed_rs
FROM main.bills_view;

-- COMMAND ----------

-- DBTITLE 1,Month-wise Consumption
CREATE OR REPLACE VIEW main.v_monthly_consumption AS
SELECT
  bill_month,
  ROUND(AVG(CAST(consumption_units AS DOUBLE)), 1)  AS avg_units,
  MIN(CAST(consumption_units AS DOUBLE))            AS min_units,
  MAX(CAST(consumption_units AS DOUBLE))            AS max_units,
  ROUND(AVG(CAST(current_amount_due AS DOUBLE)), 0) AS avg_amount,
  COUNT(*)                                          AS bill_count
FROM main.bills_view
GROUP BY bill_month

-- COMMAND ----------

-- Chart: Bar | X: bill_month | Y: avg_units
SELECT * FROM main.v_monthly_consumption ORDER BY bill_month;

-- COMMAND ----------

-- DBTITLE 1,Cost per Unit Trend
CREATE OR REPLACE VIEW main.v_cost_per_unit AS
SELECT
  bill_month,
  avg_units,
  avg_amount,
  ROUND(avg_amount / NULLIF(avg_units, 0), 2) AS rs_per_unit
FROM main.v_monthly_consumption
ORDER BY bill_month;

-- COMMAND ----------

-- Chart: Line | X: bill_month | Y: rs_per_unit
SELECT * FROM main.v_cost_per_unit;

-- COMMAND ----------

-- DBTITLE 1,Summer vs Winter Comparison
CREATE OR REPLACE VIEW main.v_seasonal_usage AS
SELECT
  CASE
    WHEN MONTH(bill_issue_date) IN (4,5,6,7)    THEN 'Summer (Apr-Jul)'
    WHEN MONTH(bill_issue_date) IN (11,12,1,2)  THEN 'Winter (Nov-Feb)'
    ELSE                                              'Moderate'
  END                                               AS season,
  ROUND(AVG(consumption_units), 1)                  AS avg_units,
  ROUND(AVG(current_amount_due), 0)                 AS avg_amount,
  COUNT(*)                                          AS bills
FROM main.bills_view
GROUP BY season
ORDER BY avg_units DESC;



-- COMMAND ----------

-- Chart: Bar | X: season | Y: avg_units
SELECT * FROM main.v_seasonal_usage;

-- COMMAND ----------

-- DBTITLE 1,Final Summary KPIs
SELECT
  ROUND(AVG(consumption_units), 1)                              AS avg_monthly_units,
  ROUND(AVG(current_amount_due), 0)                             AS avg_monthly_bill_rs,
  ROUND(AVG(current_amount_due / NULLIF(consumption_units,0)),2) AS avg_rs_per_unit,
  ROUND(SUM(current_amount_due), 0)                             AS total_paid_rs,
  ROUND(SUM(tariff_subsidy), 0)                                 AS total_subsidy_rs,
  COUNT(*)                                                      AS months_analysed
FROM main.bills_view;

-- COMMAND ----------

SELECT
  bill_month,
  consumption_units,
  current_amount_due,
  CASE
    WHEN consumption_units = MAX(consumption_units) OVER() THEN 'Peak Month'
    WHEN consumption_units = MIN(consumption_units) OVER() THEN 'Lowest Month'
    ELSE 'Normal'
  END AS label
FROM main.bills_view
ORDER BY consumption_units DESC;

-- COMMAND ----------

-- DBTITLE 1,AI Assistant Live Demo Prompts
-- PROMPT 1: "Explain what v_monthly_consumption is doing in simple words"
-- PROMPT 2: "Write a query comparing highest vs lowest consumption month"
-- PROMPT 3: "Which month did I overpay the most per unit?"
-- PROMPT 4: "How to automate this pipeline to run every month?"