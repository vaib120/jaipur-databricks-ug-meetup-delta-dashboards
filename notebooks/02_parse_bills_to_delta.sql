-- Databricks notebook source
-- DBTITLE 1,Use Catalog & Schema
USE CATALOG demo;
USE SCHEMA main;

-- COMMAND ----------

-- DBTITLE 1,OCR with ai_parse_document
CREATE OR REPLACE TABLE main.bills_parsed AS
SELECT
  path,
  ai_parse_document(
    content,
    map('version', '2.0', 'language', 'hi')
  ) AS parsed_content
FROM main.raw_bills;

-- COMMAND ----------

select path,parsed_content from main.bills_parsed

-- COMMAND ----------

-- DBTITLE 1,Select bills_parsed
SELECT
  path,
  parsed_content:document:elements AS pages
FROM main.bills_parsed;

-- COMMAND ----------

-- DBTITLE 1,Flatten Elements to Plain Text
CREATE OR REPLACE TABLE main.bills_text AS
SELECT
  path,
  concat_ws(
    '\n',
    transform(
      try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
      e -> try_cast(e:content AS STRING)
    )
  ) AS bill_text
FROM main.bills_parsed;

-- COMMAND ----------

-- DBTITLE 1,Select bills_text
SELECT
  path,
  substr(bill_text, 1, 600) AS text_preview
FROM main.bills_text
LIMIT 3;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bills_html AS
SELECT 
path,
try_cast(e:content AS STRING) as table_html
from main.bills_parsed
LATERAL VIEW explode(try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)) exploded as e 
WHERE try_cast(e:type AS STRING) = 'table'

-- COMMAND ----------

select *,
ai_extract(table_html, array('ई-मेल'))
 from main.bills_html

-- COMMAND ----------

select *, 
explode(regexp_extract_all(table_html, '<tr>(.*?)</tr>', 1)) as tr
 from main.bills_html

-- COMMAND ----------

with rows as (
  select *, 
explode(regexp_extract_all(table_html, '<tr>(.*?)</tr>', 1)) as tr
 from main.bills_html
)
select path, tr
from rows
where tr like '%<td>%'

-- COMMAND ----------

CREATE OR REPLACE TABLE main.electricity_bills AS
SELECT
  path,
  current_timestamp() AS processed_at,

  ai_query(
    'databricks-llama-4-maverick',
    concat(
    'You are an expert at reading JVVNL electricity bills from Rajasthan India. The bill is in Hindi and English mixed format. Extract all fields accurately and translate Hindi labels to English values. Return null for any missing fields.',
    bill_text),
    responseFormat => '{
      "type": "json_schema",
      "json_schema": {
        "name": "jvvnl_bill",
        "schema": {
          "type": "object",
          "properties": {
            "account_number":            {"type": ["string","null"]},
            "bill_month":                {"type": ["string","null"]},
            "bill_issue_date":           {"type": ["string","null"]},
            "due_date":                  {"type": ["string","null"]},
            "consumption_units":         {"type": ["number","null"]},
            "current_reading_kwh":       {"type": ["number","null"]},
            "previous_reading_kwh":      {"type": ["number","null"]},
            "energy_charges":            {"type": ["number","null"]},
            "fixed_charges":             {"type": ["number","null"]},
            "fuel_regulatory_surcharge": {"type": ["number","null"]},
            "corporation_amount":        {"type": ["number","null"]},
            "electricity_duty":          {"type": ["number","null"]},
            "water_conservation_cess":   {"type": ["number","null"]},
            "urban_cess":                {"type": ["number","null"]},
            "tariff_subsidy":            {"type": ["number","null"]},
            "cm_kisan_mitra_subsidy":    {"type": ["number","null"]},
            "current_amount_due":        {"type": ["number","null"]},
            "total_amount_due":          {"type": ["number","null"]},
            "late_payment_surcharge":    {"type": ["number","null"]},
            "last_payment_amount":       {"type": ["number","null"]},
            "last_payment_date":         {"type": ["string","null"]},
            "meter_number":              {"type": ["string","null"]},
            "sanctioned_category":       {"type": ["string","null"]},
            "tariff_code":               {"type": ["string","null"]},
            "security_deposit":          {"type": ["number","null"]},
            "avg_monthly_consumption":   {"type": ["number","null"]},
            "urban_rural":               {"type": ["string","null"]}
          },
          "additionalProperties": false
        },
        "strict": true
      }
    }'
  ) AS bill
FROM main.bills_text;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.electricity_bills AS

SELECT
  path,
  current_timestamp() AS processed_at,

  ai_query(
    'databricks-llama-4-maverick',
    'You are an expert at reading JVVNL electricity bills from Rajasthan India. The bill is in Hindi and English mixed format. Extract all fields accurately and translate Hindi labels to English values. Return null for any missing fields. उपभोग is consumption_units for example उपभोग : 266.0000,राज्य सरकार द्वारा वहन राशि (1) टैरिफ सब्सिडी as tariff_subsidy,औसत मासिक उपभोग (यूनिट) is avg_monthly_consumption, नियत तिथि तक कुल देय राशि is current_amount_due and कुल उपभोग राशि (क्रम 11 से 17 तक का योग) as total_amount_due',
    responseFormat => '{
      "type": "json_schema",
      "json_schema": {
        "name": "jvvnl_bill",
        "schema": {
          "type": "object",
          "properties": {
            "account_number":            {"type": ["string","null"]},
            "bill_month":                {"type": ["string","null"]},
            "bill_issue_date":           {"type": ["string","null"]},
            "due_date":                  {"type": ["string","null"]},
            "consumption_units":         {"type": ["number","null"]},
            "current_reading_kwh":       {"type": ["number","null"]},
            "previous_reading_kwh":      {"type": ["number","null"]},
            "energy_charges":            {"type": ["number","null"]},
            "fixed_charges":             {"type": ["number","null"]},
            "fuel_regulatory_surcharge": {"type": ["number","null"]},
            "corporation_amount":        {"type": ["number","null"]},
            "electricity_duty":          {"type": ["number","null"]},
            "water_conservation_cess":   {"type": ["number","null"]},
            "urban_cess":                {"type": ["number","null"]},
            "avg_monthly_consumption":   {"type": ["number","null"]},
            "tariff_subsidy":            {"type": ["number","null"]},
            "cm_kisan_mitra_subsidy":    {"type": ["number","null"]},
            "current_amount_due":        {"type": ["number","null"]},
            "total_amount_due":          {"type": ["number","null"]},
            "late_payment_surcharge":    {"type": ["number","null"]},
            "last_payment_amount":       {"type": ["number","null"]},
            "last_payment_date":         {"type": ["string","null"]},
            "meter_number":              {"type": ["string","null"]},
            "sanctioned_category":       {"type": ["string","null"]},
            "tariff_code":               {"type": ["string","null"]},
            "security_deposit":          {"type": ["number","null"]},
            "urban_rural":               {"type": ["string","null"]}
          },
          "additionalProperties": false
        },
        "strict": true
      }
    }',
    files => content
  ) AS bill

FROM READ_FILES(
  '/Volumes/main/electricity/raw_bills/',
  format => 'binaryFile'
);

-- COMMAND ----------

-- DBTITLE 1,Clean Delta Table with Proper Types
CREATE OR REPLACE VIEW main.bills_view AS
SELECT
  path,
  processed_at,
  get_json_object(bill, '$.account_number')                        AS account_number,
  get_json_object(bill, '$.bill_month')                            AS bill_month,
  get_json_object(bill, '$.meter_number')                          AS meter_number,
  get_json_object(bill, '$.sanctioned_category')                   AS sanctioned_category,
  try_to_date(get_json_object(bill, '$.bill_issue_date'),  'dd-MM-yyyy') AS bill_issue_date,
  try_to_date(get_json_object(bill, '$.due_date'),         'dd-MM-yyyy') AS due_date,
  try_to_date(get_json_object(bill, '$.last_payment_date'),'dd-MM-yyyy') AS last_payment_date,
  try_cast(get_json_object(bill, '$.consumption_units')         AS DOUBLE) AS consumption_units,
  try_cast(get_json_object(bill, '$.current_reading_kwh')       AS DOUBLE) AS current_reading_kwh,
  try_cast(get_json_object(bill, '$.previous_reading_kwh')      AS DOUBLE) AS previous_reading_kwh,
  try_cast(get_json_object(bill, '$.energy_charges')            AS DOUBLE) AS energy_charges,
  try_cast(get_json_object(bill, '$.fixed_charges')             AS DOUBLE) AS fixed_charges,
  try_cast(get_json_object(bill, '$.fuel_regulatory_surcharge') AS DOUBLE) AS fuel_regulatory_surcharge,
  try_cast(get_json_object(bill, '$.electricity_duty')          AS DOUBLE) AS electricity_duty,
  try_cast(get_json_object(bill, '$.water_conservation_cess')   AS DOUBLE) AS water_conservation_cess,
  try_cast(get_json_object(bill, '$.urban_cess')                AS DOUBLE) AS urban_cess,
  try_cast(get_json_object(bill, '$.avg_monthly_consumption')   AS DOUBLE) AS avg_monthly_consumption,
  try_cast(get_json_object(bill, '$.corporation_amount')        AS DOUBLE) AS corporation_amount,
  try_cast(get_json_object(bill, '$.tariff_subsidy')            AS DOUBLE) AS tariff_subsidy,
  try_cast(get_json_object(bill, '$.cm_kisan_mitra_subsidy')    AS DOUBLE) AS cm_kisan_mitra_subsidy,
  try_cast(get_json_object(bill, '$.current_amount_due')        AS DOUBLE) AS current_amount_due,
  try_cast(get_json_object(bill, '$.total_amount_due')          AS DOUBLE) AS total_amount_due,
  try_cast(get_json_object(bill, '$.late_payment_surcharge')    AS DOUBLE) AS late_payment_surcharge,
  try_cast(get_json_object(bill, '$.last_payment_amount')       AS DOUBLE) AS last_payment_amount,
  try_cast(get_json_object(bill, '$.security_deposit')          AS DOUBLE) AS security_deposit
FROM main.electricity_bills;



-- COMMAND ----------

SELECT * FROM main.bills_view ORDER BY bill_issue_date;

-- COMMAND ----------

-- DBTITLE 1,Quality Check
SELECT
  COUNT(*)                                               AS total_bills,
  COUNT(account_number)                                  AS accounts_found,
  COUNT(consumption_units)                               AS units_found,
  COUNT(current_amount_due)                              AS amounts_found,
  ROUND(100.0 * COUNT(account_number)     / COUNT(*), 1) AS account_pct,
  ROUND(100.0 * COUNT(consumption_units)  / COUNT(*), 1) AS units_pct,
  ROUND(100.0 * COUNT(current_amount_due) / COUNT(*), 1) AS amount_pct
FROM main.bills_view;

-- COMMAND ----------

SELECT
  bill_month,
  consumption_units,
  avg_monthly_consumption,
  round(consumption_units - avg_monthly_consumption, 1)  AS vs_your_average,
  CASE
    WHEN consumption_units > avg_monthly_consumption * 1.2 THEN '🔴 High'
    WHEN consumption_units < avg_monthly_consumption * 0.8 THEN '🟢 Low'
    ELSE '🟡 Normal'
  END AS consumption_flag
FROM main.bills_view
ORDER BY bill_month;

-- COMMAND ----------

SELECT bill_month,
  consumption_units,
  round(energy_charges, 2)            AS energy_charges,
  round(fixed_charges, 2)             AS fixed_charges,
  round(fuel_regulatory_surcharge, 2) AS fuel_surcharge,
  round(electricity_duty, 2)          AS electricity_duty,
  round(water_conservation_cess, 2)   AS water_cess,
  round(urban_cess, 2)                AS urban_cess,
  round(tariff_subsidy * -1, 2)       AS govt_subsidy_saved,
  round(current_amount_due, 2)        AS final_amount_paid
FROM main.bills_view
ORDER BY bill_month;

-- COMMAND ----------

-- Analysis 4: Cost per unit — your effective electricity rate over time

SELECT
  bill_month,
  consumption_units,
  round(current_amount_due, 2)                                      AS amount_paid,
  round(current_amount_due / nullif(consumption_units, 0), 2)       AS cost_per_unit,
  round(energy_charges    / nullif(consumption_units, 0), 2)        AS energy_rate_per_unit,
  round(corporation_amount / nullif(consumption_units, 0), 2)       AS gross_rate_per_unit
FROM main.bills_view
ORDER BY bill_month;

-- COMMAND ----------

SELECT bill_month, category, amount
FROM (
  SELECT
    bill_month,

    map(
      'energy_charges', energy_charges,
      'fixed_charges', fixed_charges,
      'fuel_surcharge', fuel_regulatory_surcharge,
      'electricity_duty', electricity_duty,
      'water_cess', water_conservation_cess,
      'urban_cess', urban_cess,
      'govt_subsidy_saved', tariff_subsidy * -1,
      'final_amount_paid', current_amount_due
    ) AS m

  FROM main.bills_view
)
LATERAL VIEW explode(m) t AS category, amount;