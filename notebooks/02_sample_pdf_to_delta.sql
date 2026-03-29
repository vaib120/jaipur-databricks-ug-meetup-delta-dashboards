-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW raw_unstructured_doc AS
SELECT
  path,
  content
FROM read_files("/Volumes/demo/main/bills_volume/samplepdf/")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_structured_doc AS
SELECT
  path,
  ai_parse_document(content) AS parsed_content
FROM raw_unstructured_doc

-- COMMAND ----------

select * from parsed_structured_doc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC https://www.databricks.com/blog/pdfs-production-announcing-state-art-document-intelligence-databricks
-- MAGIC
-- MAGIC ### ai_parse_document 
-- MAGIC captures tables, figures, and diagrams with AI-generated descriptions and spatial metadata, storing results in Unity Catalog. 
-- MAGIC Your documents now behave like tables—searchable through Vector Search and actionable in Agent Bricks workflows.
-- MAGIC
-- MAGIC "The ai_parse_document() function invokes a (state-of-the-art generative AI model from Databricks Foundation Model APIs) to extract structured content from unstructured documents."
-- MAGIC
-- MAGIC ![image_1774800333620.png](./image_1774800333620.png "image_1774800333620.png")

-- COMMAND ----------

-- LATERAL VIEW explode is used to flatten arrays or maps in a column, creating a new row for each element.
-- Example: explode an array column 'elements' to get each element as a separate row.

CREATE OR REPLACE TEMP VIEW structured_tables AS
SELECT path,
  try_cast(e:content AS STRING) as table_html
FROM parsed_structured_doc
LATERAL VIEW explode(try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)) t as e
WHERE try_cast(e:type AS STRING) = 'table'

-- COMMAND ----------

select * from structured_tables

-- COMMAND ----------

select *, 
ai_extract(table_html,array('Disability Category')).`Disability Category` As Disability_Category,
ai_extract(table_html,array('Participants')).`Participants`  As Participants,
ai_extract(table_html,array('Ballots Completed')).`Ballots Completed`  As Ballots_Completed,
ai_extract(table_html,array('Ballots Incomplete/Terminated')).`Ballots Incomplete/Terminated`  As Ballots_Incomplete_Terminated,
ai_extract(table_html,array('Accuracy')).`Accuracy`  As Results_Accuracy,
ai_extract(table_html,array('Time to complete')).`Time to complete`  As Results_Time_to_complete
from structured_tables

-- COMMAND ----------

-- This query extracts table rows from HTML in the 'table_html' column by matching <tr> tags,
-- then filters to only rows containing <td> tags (data rows).

WITH rows as (
SELECT *,
  explode(regexp_extract_all(table_html, '<tr>(.*?)</tr>', 1)) as tr
FROM structured_tables
)
SELECT path, tr
FROM rows
WHERE tr LIKE '%<td>%'

-- COMMAND ----------

-- 'rows' CTE: Extracts all <tr> (table row) elements from the HTML in 'table_html' and explodes them into individual rows.
WITH rows AS (
  SELECT *,
    explode(regexp_extract_all(table_html, '<tr>(.*?)</tr>', 1)) AS tr
  FROM structured_tables
), 

-- 'data_rows' CTE: Filters the extracted rows to only those containing <td> (table data) tags, representing actual data rows.
data_rows AS (
  SELECT path, tr
  FROM rows
  WHERE tr LIKE '%<td>%'
), 

-- 'tds' CTE: Extracts all <td> elements from each data row into an array, mapping each cell to its column.
tds AS (
  SELECT
    path,
    tr,
    regexp_extract_all(tr, '<td>(.*?)</td>', 1) AS td_array
  FROM data_rows
)

-- Final SELECT: Assigns each cell in the array to its corresponding column name for structured output.
SELECT
  path,
  td_array[0] AS Disability_Category,
  td_array[1] AS Participants,
  td_array[2] AS Ballots_Completed,
  td_array[3] AS Ballots_Incomplete_Terminated,
  td_array[4] AS Results_Accuracy,
  td_array[5] AS Results_Time_to_complete
FROM tds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC I have an existing temp view called `structured_tables` with these columns:
-- MAGIC - `path`
-- MAGIC - `table_html`
-- MAGIC
-- MAGIC `table_html` contains HTML table markup extracted from documents.
-- MAGIC
-- MAGIC Please generate a Databricks SQL query that:
-- MAGIC 1. Reads from `structured_tables`
-- MAGIC 2. Extracts each `<tr>...</tr>` row from `table_html`
-- MAGIC 3. Keeps only rows that contain `<td>` cells, not header rows
-- MAGIC 4. Extracts all `<td>...</td>` values into an array
-- MAGIC 5. Returns one row per HTML table row
-- MAGIC 6. Selects:
-- MAGIC    - `path`
-- MAGIC    - `td_array[0] AS Disability_Category`
-- MAGIC    - `td_array[1] AS Participants`
-- MAGIC    - `td_array[2] AS Ballots_Completed`
-- MAGIC    - `td_array[3] AS Ballots_Incomplete_Terminated`
-- MAGIC    - `td_array[4] AS Results_Accuracy`
-- MAGIC    - `td_array[5] AS Results_Time_to_complete`
-- MAGIC
-- MAGIC Please use CTEs named exactly:
-- MAGIC - `rows`
-- MAGIC - `data_rows`
-- MAGIC - `tds`
-- MAGIC
-- MAGIC Please use Databricks SQL functions such as:
-- MAGIC - `explode`
-- MAGIC - `regexp_extract_all`
-- MAGIC
-- MAGIC Expected logic:
-- MAGIC - In `rows`, explode all `<tr>` matches from `table_html`
-- MAGIC - In `data_rows`, filter rows where `tr LIKE '%<td>%'`
-- MAGIC - In `tds`, extract all `<td>...</td>` values into `td_array`
-- MAGIC
-- MAGIC Return the final SELECT with the six business columns listed above.
-- MAGIC Please provide only the final SQL query, properly formatted.

-- COMMAND ----------

WITH rows AS (
  SELECT
    path,
    explode(regexp_extract_all(table_html, '<tr>(.*?)</tr>', 1)) AS tr
  FROM structured_tables
),
data_rows AS (
  SELECT
    path,
    tr
  FROM rows
  WHERE tr LIKE '%<td>%'
),
tds AS (
  SELECT
    path,
    tr,
    regexp_extract_all(tr, '<td>(.*?)</td>', 1) AS td_array
  FROM data_rows
)
SELECT
  path,
  td_array[0] AS Disability_Category,
  td_array[1] AS Participants,
  td_array[2] AS Ballots_Completed,
  td_array[3] AS Ballots_Incomplete_Terminated,
  td_array[4] AS Results_Accuracy,
  td_array[5] AS Results_Time_to_complete
FROM tds