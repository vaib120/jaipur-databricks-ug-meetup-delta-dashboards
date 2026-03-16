# Databricks User Group Jaipur – 4th Edition Meetup

This repository contains the code, notebooks, and sample data used in the **4th edition meetup of the Databricks User Group Jaipur**, held on **31 March 2026**.

In this meetup we use **Databricks Free Edition** to go from **zero setup** to **Delta tables and decision‑making dashboards** using anonymized **electricity bill PDFs**.

---

## 1. What this repo demonstrates

Based on the live sessions:

- **Session 1 – Databricks Free Edition Kickstart: From Zero Setup to Delta Lake**  
  - What Databricks Free Edition is and what you can do with it  
  - How to sign up and get your first workspace  
  - How to upload PDFs into a Unity Catalog **Volume**  
  - How to use **AI document parsing** to extract structured fields from electricity bills  
  - How to save clean data into **Delta tables**

- **Session 2 – From Delta Tables to Decision‑Making: Building Dashboards in Databricks**  
  - How to run analytics on the Delta table (usage, cost, subsidies, late fees)  
  - How to build simple but powerful **dashboards** on top of the bills data  
  - How to use **Databricks Assistant / AI features** to generate queries and explore insights

The goal of this repo is to make the entire meetup **fully reproducible** for:

- Attendees who want to replay the steps at home  
- New community members who discover **Databricks User Group Jaipur** later  
- Anyone looking for a realistic end‑to‑end **Data & AI** example on Databricks Free Edition

---

## 2. Who is this for?

- **Students** who want a *real, relatable* project (everyone has an electricity bill).  
- **Data & analytics professionals** exploring **Databricks Free Edition** and the lakehouse.  
- **Community members** who want a small but complete project they can extend.

You only need:

- A Databricks **Free Edition** account (no credit card required)  
- A modern browser and about **60–90 minutes**

---

## 3. What is Databricks Free Edition? (from the meetup)

In line with the slides:

- Full Databricks workspace with **no credit card required**  
- **Serverless compute** automatically provisioned for you  
- **Built‑in storage** for data and files  
- Access to **Unity Catalog** for basic data governance  

You can:

- Learn Databricks concepts and SQL/Python/Spark  
- Build simple data engineering pipelines with **Delta tables**  
- Create **dashboards and visualizations**  
- Experiment with **AI document parsing** and basic **AI agents**

Limitations to keep in mind (summarized):

- Small cluster size, **serverless only**, no custom cluster configs  
- Up to a few concurrent jobs (no heavy parallel workloads)  
- Restricted outbound internet, no direct external cloud storage (S3/ADLS)  
- No production‑grade ML features like model serving

This project is designed to **respect those limits** and still show an impactful end‑to‑end flow.

---

## 4. Project structure

```text
notebooks/
  01_free_edition_setup_and_ingest.sql
  02_parse_bills_to_delta.sql
  03_dashboards_and_ai_assistant.sql

sample-data/
  bills-anonymized/
    bill_202510_sample.pdf
    bill_202511_sample.pdf
    bill_202602_sample.pdf

images/
  architecture-overview.png
  sample-dashboard.png

```

---

## 5. Step‑by‑step: from sign‑up to Delta tables and dashboards
  5.1 Sign up for Databricks Free Edition
    Go to the Free Edition page:
    
    https://www.databricks.com/learn/free-edition
    
  - Click Start for free and create your account (email + password).
    
  - After login, you land in your Free Edition workspace with:    
    - A default catalog (e.g. main)    
    - Acess to Unity Catalog and Volumes
    - Serverless compute available for notebooks

          If you already have Free Edition, just log in and go to the workspace home.  
