# AWS Supply Chain Analytics Pipeline

An end-to-end **AWS Supply Chain Analytics Pipeline** that transforms raw supply chain data into analytics-ready datasets and interactive business intelligence dashboards.

The project demonstrates a complete data engineering and analytics workflow using Amazon S3, AWS Glue, PySpark, AWS Glue Data Catalog, Amazon Athena, Parquet, SQL, and Power BI.

---

Project Overview

Supply chain data is often distributed across multiple datasets such as customers, orders, products, inventory, suppliers, warehouses, carriers, and shipments.

This project builds a cloud-based analytics pipeline to:

- Ingest raw supply chain CSV files into Amazon S3
- Catalog datasets using AWS Glue Crawlers
- Transform and integrate datasets using AWS Glue and PySpark
- Perform data quality and validation checks
- Store processed and curated datasets in Parquet format
- Query analytics-ready data using Amazon Athena
- Build business KPIs and interactive dashboards using Power BI

---

Architecture

```text
Raw CSV Files
      |
      v
Amazon S3 - raw/
      |
      v
AWS Glue Crawler
      |
      v
AWS Glue Data Catalog
      |
      v
AWS Glue PySpark ETL
      |
      +--------------------+
      |                    |
      v                    v
Data Validation       Data Transformation
      |                    |
      +---------+----------+
                |
                v
        Processed Parquet
                |
                v
        Curated Analytics
                |
                v
          Amazon Athena
                |
                v
            Power BI
                |
                v
 Supply Chain Analytics Dashboard
```

---

Technology Stack

| Technology | Purpose |
|---|---|
| Amazon S3 | Raw, processed, and curated data storage |
| AWS Glue | ETL pipeline development |
| AWS Glue Crawlers | Automatic schema discovery |
| AWS Glue Data Catalog | Centralized metadata catalog |
| PySpark | Large-scale data transformation |
| Parquet | Optimized columnar storage |
| Amazon Athena | Serverless SQL analytics |
| SQL | Data validation and business analysis |
| Power BI | KPI reporting and visualization |

---

Source Data

The pipeline processes multiple supply chain datasets:

- Customers
- Orders
- Products
- Inventory
- Shipments
- Suppliers
- Warehouses
- Carriers

Raw datasets are stored in the Amazon S3 `raw/` layer.

---

Data Pipeline

1. Data Ingestion

Raw CSV datasets are uploaded into Amazon S3 using a structured folder hierarchy.

```text
raw/
├── Customers/
├── Inventory/
├── Orders/
├── Products/
├── Shipments/
├── Suppliers/
├── Warehouses/
└── Carriers/
```

2. Schema Discovery

AWS Glue Crawlers scan the raw S3 datasets and register their schemas in the AWS Glue Data Catalog.

Custom CSV classifiers can be used where required to ensure correct header and schema detection.

3. ETL Processing

AWS Glue PySpark jobs transform the raw datasets into analytics-ready data.

Transformations include:

- Data type standardization
- Date conversions
- Dataset joins
- Revenue calculations
- Delivery-time calculations
- Shipment validation
- Missing-value checks
- Duplicate detection
- Business-rule validation

4. Processed Data

Processed datasets are stored in **Apache Parquet** format to improve query performance and reduce unnecessary data scanning.

5. Analytics Layer

Amazon Athena provides serverless SQL querying over the curated supply chain dataset.

The analytics output is then consumed by Power BI for reporting.

---

Data Quality

The pipeline includes validation checks for:

- Null values
- Duplicate records
- Missing shipment information
- Invalid identifiers
- Data type consistency
- Date consistency
- Record reconciliation

These checks help ensure that downstream reporting is based on reliable data.

---

Power BI Dashboard

The Power BI dashboard provides an interactive view of supply chain performance.

KPI Cards

- Total Revenue
- Total Orders
- Average Order Value
- Average Delivery Days
- Orders Missing Shipment
- Missing Shipment %

Business Analysis

The dashboard includes:

- Revenue by Product Category
- Revenue by Month
- Orders by Order Status
- Revenue by Warehouse City
- Revenue by Customer Segment

Interactive Filters

Users can filter the dashboard by:

- Warehouse City
- Order Status
- Product Category
- Order Date

---

## Dashboard Preview

A screenshot of the completed Power BI dashboard will be added here.

```text
docs/
└── powerbi-dashboard.png
```

---

Example Business Insights

The dashboard enables analysts and business stakeholders to:

- Identify high-performing product categories
- Monitor monthly revenue trends
- Compare warehouse performance
- Analyze customer segments
- Monitor order fulfillment status
- Identify orders with missing shipment records
- Track average delivery performance

---

Repository Structure

```text
aws-supply-chain-analytics-pipeline/
│
├── data/
│   └── sample/
│
├── glue/
│   └── supply_chain_etl.py
│
├── sql/
│   ├── validation_queries.sql
│   └── analytics_queries.sql
│
├── docs/
│   └── powerbi-dashboard.png
│
├── README.md
├── .gitignore
└── LICENSE
```

---

Skills Demonstrated

This project demonstrates practical experience with:

Data Engineering
- ETL pipeline development
- AWS Glue
- PySpark
- Amazon S3
- Parquet
- Data transformation

Data Quality
- Data validation
- Duplicate detection
- Null handling
- Reconciliation
- Pipeline quality checks

Analytics
- Amazon Athena
- SQL
- KPI development
- Supply chain analytics

Business Intelligence
- Power BI
- DAX measures
- Interactive dashboards
- Data visualization
- Business reporting

---

Future Enhancements

Potential improvements include:

- Event-driven pipeline execution using Amazon EventBridge
- AWS Lambda-based automation
- CloudWatch monitoring and alerting
- Automated data quality reporting
- CI/CD pipeline deployment
- Incremental data processing
- Pipeline orchestration
- Additional operational KPIs

---

Author

Satish Ratakonda

Data Analyst | Data Engineer | BI Analyst

Skills: SQL • Python • PySpark • Power BI • AWS • Databricks • ETL • Data Quality
