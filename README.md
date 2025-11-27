# Log Analyser

## **Overview**

**Log Analyser** is a PySpark-based pipeline designed to parse, clean, categorize, and analyze application logs in a structured manner. Modern distributed systems—especially microservices and containerized workloads—generate large volumes of logs, making manual inspection inefficient and error-prone. This project automates the extraction of meaningful information from raw JSON logs and provides insights that help engineers quickly identify issues, patterns, and trends.

## **Problem Statement**

In real engineering environments, logs often contain large amounts of unstructured text. Reviewing them manually is slow and can delay issue resolution. Engineers typically need answers to questions like:

* *Which services are generating the most errors?*
* *When do errors peak during the day?*
* *Is an issue related to timeouts, authentication failures, caching, or database connectivity?*
* *What were the most recent failures?*

This project addresses these challenges through a structured log analysis approach.

## **Project Description**

The **Log Analyser** pipeline reads raw JSON logs, applies a custom parser to extract relevant fields, enriches the data with additional metadata, and generates summaries that highlight key operational insights. The processing is implemented using **PySpark**, enabling scalable handling of large log volumes.

The pipeline performs the following operations:

### **1. Log Parsing**

Raw log entries are parsed using a custom UDF (`parse_json_log`).
Each log is transformed into a consistent schema containing:

* `timestamp`
* `loggerType`
* `message`
* `loggerName`
* `filename`

This ensures all logs follow a normalized format suitable for analysis.

### **2. Data Enrichment**

Additional columns are derived using PySpark functions:

* **Timestamp conversion**
* **Date and hour extraction**
* **Categorizing each log** using rules based on message content
  (e.g., DB errors, timeouts, authentication failures)

This enrichment allows for detailed trend analysis.

### **3. Important Logs Extraction**

The system filters and extracts significant log entries—specifically those with:

* `WARN`
* `ERROR`

These logs are written into separate Parquet files to support fast retrieval and further analysis.

### **4. Trend & Summary Insights**

The pipeline generates multiple insights:

* **Error counts per service**
* **Top latest errors**
* **Category-wise error classification**

These outputs help engineers identify hotspots and recurring fault patterns.

### **5. Storage (Parquet)**

Processed logs are saved in **Parquet format**, a columnar storage format widely used in data engineering.
This makes further processing efficient and integrates well with downstream systems such as Spark SQL, Databricks, and cloud analytics services.

---

## **Impact**

This project improves engineering efficiency by:

* Reducing time spent on manual log inspection
* Providing structured insights that help in quickly diagnosing issues
* Enabling scalable log analysis for large datasets
* Creating a foundation for building dashboards, alerts, or automated monitoring pipelines
---

## **Technologies Used**

* **Python 3**
* **PySpark**
* **Parquet**
* **UDF-based parsing**
* **Spark DataFrames and SQL functions**



