# Databricks User Group Jaipur – 4th Edition Meetup

This repository contains the code, notebooks, and sample data used in the **4th edition meetup of the Databricks User Group Jaipur**.

End-to-end **Databricks Free Edition** project used in the *Databricks User Group Jaipur* meetup on **31 March 2026**.

We show how to go from **zero setup** to **Delta tables and dashboards** using anonymized **electricity bill PDFs**:

- Upload PDFs to a Unity Catalog **Volume**
- Turn real-world **electricity bill PDFs** into clean **Delta tables**  
- Use **AI document parsing** to extract structured fields
- Store clean data in **Delta tables**
- Build **dashboards** and ask **AI-assisted questions** on top

The goal of this repo is to make the entire meetup **reproducible** for:
- Attendees who want to replay the steps at home  
- New community members who discover the Databricks User Group Jaipur later  
- Anyone looking for a simple but realistic end-to-end Data & AI example on Databricks

---

## 1. Who is this for?

- Students who want a *real, relatable* project (everyone has an electricity bill).
- Data & analytics professionals exploring **Databricks Free Edition**.
- Community members who want a reproducible project they can extend.

You only need:

- A Databricks **Free Edition** account (no credit card)  
- A browser and ~60–90 minutes

---

## 2. Project structure

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
