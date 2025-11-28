# 🏥 FHIR Healthcare Data Warehouse For Analytics
End‑to‑End Pipeline: FHIR → Ingestion → Lakehouse → DW → Power BI (another repo)

This project builds a complete modern healthcare analytics platform using FHIR resources, Lakehouse architecture, and Gold‑layer dimensional modeling.
The goal is to transform raw clinical FHIR bundles into an analytics‑ready data warehouse powering executive dashboards.

Includes:
- **Downloaded FHIR file** → Synthetic data from Synthea: https://synthea.mitre.org/downloads
- **Python** → Clean and import data to database
- **SQL** → create stored procedure to migrate data to tables in different layers
- **MS SQL Server** → Database management

---
🏗️ Architecture Overview

```
                ┌──────────────────────────┐
                │        FHIR API /        │
                │    Downloaded Bundles    │
                │ (Patient, Encounter, ...)│
                └─────────────┬────────────┘
                              │ Raw JSON
                              ▼
                   ┌─────────────────────┐
                   │      BRONZE         │
                   │ Raw FHIR structure  │
                   │ (JSON, no schema)   │
                   └──────────┬──────────┘
                              │ Flatten/Normalize
                              ▼
                   ┌─────────────────────┐
                   │      SILVER         │
                   │ Cleaned tables:     │
                   │  patient, encounter │
                   │  observation, etc   │
                   └──────────┬──────────┘
                              │ Transform to facts/dims
                              ▼
                   ┌─────────────────────┐
                   │       GOLD          │
                   │ Star Schema DW      │
                   │ Facts & Dimensions  │
                   └──────────┬──────────┘
                              │ SQL + BI
                              ▼
                     ┌─────────────────┐
                     │   Power BI      │
                     │ Executive Report│
                     └─────────────────┘

```

---

## 📂 Repository Structure

```
fhir-healthcare-warehouse/
├── README.md
│
├── data/
│   ├── raw_fhir/                 # Raw downloaded FHIR JSON bundles
│   ├── bronze/                   # Raw ingested data
│   ├── silver/                   # Flattened FHIR resources
│   └── gold/                     # Star-schema DW tables
│
├── notebooks/
│   ├── 01_download_fhir.ipynb
│   ├── 02_bronze_ingestion.ipynb
│   ├── 03_silver_transform.ipynb
│   └── 04_gold_modeling.ipynb
│
├── sql/
│   ├── dim_patient.sql
│   ├── dim_practitioner.sql
│   ├── dim_code.sql
│   ├── fact_encounter.sql
│   ├── fact_observation.sql
│   ├── fact_condition.sql
│   ├── fact_procedure.sql
│   └── fact_medication.sql
│
├── powerbi/
│   └── FHIR_Executive_Dashboard.pbix
│
└── documentation/
    ├── glossary_of_terms.md
    ├── fhir_mapping.md
    └── data_model.png (optional)


````

---

## 🛠️ Tools
- **Language**: Python 3.x, SQL
- **Data warehouse**: MS SQL Server  
- **Data transformation**: SQL   
- **Version control**: GitHub  

---

## 🧩 FHIR Resources Used

```| FHIR Resource         | Purpose                    |
| --------------------- | -------------------------- |
| **Patient**           | Demographics, identifiers  |
| **Encounter**         | Visit type, class, period  |
| **Observation**       | Labs, vitals, measurements |
| **Condition**         | Diagnoses (ICD/SNOMED)     |
| **Procedure**         | Performed procedures       |
| **MedicationRequest** | Prescriptions              |
| **Practitioner**      | Provider identities        |

````

---

### 🛢️ Warehouse Layers

<img width="1024" height="1024" alt="layers" src="https://github.com/user-attachments/assets/ec6da2a4-e52e-4e08-a9a9-6353c8d5f72b" />



---

### Data Analytics: check out this repo https://github.com/huong-nguyen17/HealthCare-Project

---


## 📜 License

[MIT](/LICENSE)

---

## 📬 Contact: https://www.linkedin.com/in/huong-tris-n-847067111/
