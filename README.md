# sql-data-warehouse-project
Building a Data Warehouse with SQL Server, using ETL processes, data modeling and analytics

# 📊 Data Warehouse with SQL Server

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?logo=microsoft-sql-server&logoColor=white)  
![ETL](https://img.shields.io/badge/ETL-Data%20Pipelines-blue?logo=apache-airflow&logoColor=white)  
![Data Modeling](https://img.shields.io/badge/Data%20Modeling-ER--Diagrams-green?logo=databricks&logoColor=white)  
![Analytics](https://img.shields.io/badge/Analytics-PowerBI-yellow?logo=powerbi&logoColor=black)

---

## 🏗️ Overview
This project implements a **Data Warehouse** using **Microsoft SQL Server** as the foundation.  
It includes **ETL processes**, robust **data modeling**, and end-to-end **analytics** for decision-making.  
This project serves as a portfolio example, highlighting best practices in data engineering and analytics.

---

## ⚙️ Specifications
- 🗄 **SQL Server-based Data Warehouse**  
- 🔄 **ETL Pipelines** for data extraction, transformation, and loading  
- 🧩 **Star Schema Modeling**  
- 📈 **Analytics & Reporting Layer** (e.g., Power BI / SSRS)  
- ✅ **Data Quality & Governance Rules**: Cleansing & resolving data quality issues prior to the analysis

---
## 📂 Architecture – Medallion

The data warehouse follows the **Medallion Architecture** pattern with three main layers: **Bronze, Silver, and Gold**.

```mermaid
flowchart LR
    %% Source layer
    subgraph Source[📂 Source Systems]
        direction TB
        A1["🗂️ CRM, ERP, CSV Files"]
    end

    %% Bronze layer
    subgraph Bronze[🥉 Bronze Layer]
        direction TB
        B1["📥 Raw Data"]
        B2["⚡ Batch Processing (Full Load)"]
        B3["❌ No Transformations"]
        B4["📝 As-is Tables"]
    end

    %% Silver layer
    subgraph Silver[🥈 Silver Layer]
        direction TB
        C1["⚡ Batch Processing (Full Load)"]
        C2["🧼 Data Cleansing"]
        C3["🔧 Standardization"]
        C4["➕ Derived Columns"]
        C5["✨ Enrichment"]
    end

    %% Gold layer
    subgraph Gold[🥇 Gold Layer]
        direction TB
        D1["📊 Business-Ready Data"]
        D2["🔗 Integrations"]
        D3["📈 Aggregations"]
        D4["🧠 Business Logic"]
        D5["⭐ Enrichment"]
        D6["🏗️ Models: Star Schema, Flat, Aggregated Tables"]
    end

    %% Consumers
    subgraph Consumers[📊 Consumers]
        direction TB
        E1["💻 SQL Server DW, Power BI, SSRS, Tableau"]
    end

    %% Tight connections between layers (only first-to-last nodes)
    A1 --> B1
    B4 --> C1
    C5 --> D1
    D6 --> E1

    %% Styling nodes with soft colors
    style A1 fill:#E0F7FA80,stroke:#006064,stroke-width:2px
    style B1 fill:#CD7F3280,stroke:#8B4513,stroke-width:2px
    style B2 fill:#CD7F3280,stroke:#8B4513,stroke-width:2px
    style B3 fill:#CD7F3280,stroke:#8B4513,stroke-width:2px
    style B4 fill:#CD7F3280,stroke:#8B4513,stroke-width:2px
    style C1 fill:#C0C0C080,stroke:#808080,stroke-width:2px
    style C2 fill:#C0C0C080,stroke:#808080,stroke-width:2px
    style C3 fill:#C0C0C080,stroke:#808080,stroke-width:2px
    style C4 fill:#C0C0C080,stroke:#808080,stroke-width:2px
    style C5 fill:#C0C0C080,stroke:#808080,stroke-width:2px
    style D1 fill:#FFD70080,stroke:#B8860B,stroke-width:2px
    style D2 fill:#FFD70080,stroke:#B8860B,stroke-width:2px
    style D3 fill:#FFD70080,stroke:#B8860B,stroke-width:2px
    style D4 fill:#FFD70080,stroke:#B8860B,stroke-width:2px
    style D5 fill:#FFD70080,stroke:#B8860B,stroke-width:2px
    style D6 fill:#FFD70080,stroke:#B8860B,stroke-width:2px
    style E1 fill:#E1F5FE80,stroke:#0277BD,stroke-width:2px
```
---
## 👋 About Me

Hi! I'm **André Graça** – a passionate developer 💻 with some experience in **MySQL** 🗄️ and **SQL** 🗄️. I love exploring data 📊 and am excited to embark on the **data engineering / data analysis journey** 🚀, turning raw information into actionable insights.

---

## 📝 License

This project is licensed under the **MIT License** [📄](https://opensource.org/licenses/MIT).

