-- Databricks notebook source
-- DBTITLE 1,Create Catalog & Schema
CREATE CATALOG IF NOT EXISTS demo;
USE CATALOG demo;
CREATE SCHEMA IF NOT EXISTS main;
USE SCHEMA main;

-- COMMAND ----------

-- DBTITLE 1,Create Volume
CREATE VOLUME IF NOT EXISTS main.bills_volume;

-- COMMAND ----------

-- DBTITLE 1,Verify Volume Path
SELECT 'Volume ready at: /Volumes/demo/main/bills_volume/' AS message;
-- ACTION: Upload your 4 JVVNL PDF bills to this path now.

-- COMMAND ----------

-- DBTITLE 1,Ingest PDFs as Binary
CREATE OR REPLACE TABLE main.raw_bills AS
SELECT
  path,
  content
FROM READ_FILES(
  '/Volumes/demo/main/bills_volume/',
  format => 'binaryFile',
  fileNamePattern => '*.pdf'
);

-- COMMAND ----------

-- DBTITLE 1,Verify Ingestion
SELECT
  COUNT(*)                          AS total_bills,
  ROUND(AVG(len(content))/1024, 1) AS avg_size_kb
FROM main.raw_bills;

-- COMMAND ----------

SELECT
  path,
  ROUND(AVG(len(content))/1024, 1) AS avg_size_kb
FROM main.raw_bills
GROUP by path;