# ETL Pipeline for Healthcare Data using Azure Databricks

## Bootcamp Project - 2

## Project Overview

This project demonstrates an end-to-end ETL pipeline for healthcare patient data using Azure cloud services. The main goal is to ingest raw patient records, clean and transform the data using Azure Databricks and PySpark, validate the transformed data, store the cleaned output in Azure Data Lake Storage, and load the final processed data into Azure SQL Database for reporting and analytics.

The project is designed for a healthcare provider that wants to process large-scale patient records for predictive analytics and regulatory reporting.

---

## Problem Statement

A healthcare provider wants to process large-scale patient records for predictive analytics and regulatory reporting. The raw patient data may contain duplicate records, missing values, inconsistent data types, and unstructured formats. This project builds a simple ETL pipeline to clean, transform, validate, and store the data in a structured format.

---

## Tools and Technologies Used

- Azure Data Lake Storage Gen2
- Azure Databricks
- Azure Data Factory
- Azure SQL Database
- Python
- PySpark
- SQL
- GitHub

---

## Project Architecture

```text
Local CSV File
sample_patient_records.csv
        ↓
Azure Data Lake Storage Gen2
raw container
        ↓
Azure Data Factory
Copy Activity Pipeline
        ↓
Azure Data Lake Storage Gen2
processed container
        ↓
Azure Databricks
PySpark Cleaning and Transformation
        ↓
Azure Data Lake Storage Gen2
processed container as Parquet
        ↓
Azure SQL Database
cleaned_patient_records table
        ↓
SQL Queries / Dashboard / Reporting
```

---

## Folder Structure

```text
Healthcare_ETL_Pipeline
│
├── README.md ( Docuemntation, scripts, codes)
│
├── sample_patient_records.csv
│   
└── Screenshots

```

---

## Sample Dataset

The project uses a sample patient dataset named:

```text
sample_patient_records.csv
```

The dataset contains the following fields:

- patient_id
- first_name
- last_name
- age
- gender
- diagnosis
- admission_date
- discharge_date
- blood_pressure
- cholesterol
- diabetes
- risk_score

The dataset intentionally includes one duplicate record and one missing age value to demonstrate data cleaning and validation.

---

## Sample CSV Data

```csv
patient_id,first_name,last_name,age,gender,diagnosis,admission_date,discharge_date,blood_pressure,cholesterol,diabetes,risk_score
P001,John,Smith,45,Male,Hypertension,2024-01-05,2024-01-10,140,220,No,70
P002,Mary,Johnson,62,Female,Diabetes,2024-01-07,2024-01-14,150,240,Yes,85
P003,David,Brown,38,Male,Asthma,2024-01-09,2024-01-11,120,180,No,40
P004,Linda,Davis,55,Female,Heart Disease,2024-01-12,2024-01-20,160,260,Yes,90
P005,James,Wilson,29,Male,Flu,2024-01-15,2024-01-17,118,170,No,25
P006,Susan,Miller,71,Female,Hypertension,2024-01-18,2024-01-24,155,230,Yes,88
P007,Robert,Taylor,50,Male,Diabetes,2024-01-20,2024-01-28,145,250,Yes,80
P008,Karen,Anderson,,Female,Asthma,2024-01-22,2024-01-25,125,190,No,45
P009,Michael,Thomas,44,Male,Hypertension,2024-01-25,2024-01-30,138,210,No,68
P009,Michael,Thomas,44,Male,Hypertension,2024-01-25,2024-01-30,138,210,No,68
```

---

# Step-by-Step Project Implementation

---

## Step 1: Create Azure Resource Group

A resource group was created to keep all project resources organized in one place.

Configuration used:

```text
Resource Group Name: healthcare-etl-rg
Region: Canada Central
```

The resource group contains:

- Azure Storage Account
- Azure Databricks Workspace
- Azure SQL Database
- Azure Data Factory

Screenshot:

```text
Step_1_Screenshot_1_Resource_Group.png
```

---

## Step 2: Create Azure Data Lake Storage Gen2

A storage account was created to store raw and processed healthcare data.

Configuration used:

```text
Storage Account Name: healthcareetlstorage
Storage Type: Azure Blob Storage or Azure Data Lake Storage Gen2
Performance: Standard
Redundancy: Locally-redundant storage LRS
Hierarchical Namespace: Enabled
```

The hierarchical namespace was enabled so that the storage account works as Azure Data Lake Storage Gen2.

Screenshot:

```text
Step_2_Screenshot_1_Storage_Account_Created.png
```

---

## Step 3: Create ADLS Containers

Two containers were created inside the storage account.

```text
raw
processed
```

Purpose of each container:

```text
raw: Stores the original patient CSV file.
processed: Stores copied and transformed output files.
```

Screenshots:

```text
Step_3_Screenshot_1_Raw_Container.png
Step_3_Screenshot_2_Processed_Container.png
```

---

## Step 4: Upload Raw Patient CSV File

The sample patient CSV file was uploaded into the `raw` container.

File uploaded:

```text
sample_patient_records.csv
```

Path:

```text
raw/sample_patient_records.csv
```

Screenshot:

```text
Step_4_Screenshot_1_Raw_CSV_Uploaded.png
```

---

## Step 5: Create Azure Databricks Workspace

An Azure Databricks workspace was created to run PySpark notebooks for data cleaning and transformation.

Configuration used:

```text
Workspace Name: healthcare-etl-databricks
Resource Group: healthcare-etl-rg
Region: Canada Central
Pricing Tier: Standard or Premium
Workspace Type: Hybrid
```

Screenshot:

```text
Step_5_Screenshot_1_Databricks_Workspace.png
```

---

## Step 6: Create Databricks Cluster

A Databricks compute cluster was created to execute the PySpark code.

Configuration used:

```text
Cluster Name: healthcare-etl-cluster
Databricks Runtime: Latest LTS Runtime
Auto Termination: 15 minutes
```

Screenshot:

```text
Step_6_Screenshot_1_Databricks_Cluster.png
```

---

## Step 7: Create Databricks Notebook

A Databricks notebook was created for the ETL transformation process.

Notebook details:

```text
Notebook Name: healthcare_etl_transformation
Language: Python
Cluster: healthcare-etl-cluster
```

---

# Databricks PySpark Scripts

---

## Script 1: Connect Databricks to ADLS Gen2 and Read Raw CSV

This script connects Azure Databricks to Azure Data Lake Storage Gen2 using the storage account key. Then it reads the raw patient CSV file from the `raw` container.

Important: Do not upload the real storage account key to GitHub. Replace it with a placeholder before submitting.

```python
storage_account_name = "healthcareetlstorage"
raw_container_name = "raw"
file_name = "sample_patient_records.csv"

storage_account_key = "PASTE_YOUR_STORAGE_ACCOUNT_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

raw_file_path = f"abfss://{raw_container_name}@{storage_account_name}.dfs.core.windows.net/{file_name}"

df = spark.read.option("header", True).option("inferSchema", True).csv(raw_file_path)

display(df)
```

Explanation:

```text
storage_account_name: Name of the Azure Storage Account.
raw_container_name: Name of the ADLS container where the raw CSV is stored.
file_name: Name of the patient CSV file.
storage_account_key: Access key used by Databricks to connect to ADLS.
spark.conf.set(): Configures Spark to access the storage account.
raw_file_path: Creates the ABFSS path to the CSV file.
spark.read.csv(): Reads the CSV file into a Spark DataFrame.
display(df): Displays the raw data in Databricks.
```

Screenshot:

```text
Step_7_Screenshot_1_Databricks_Read_Raw_Data.png
```

---

## Script 2: Data Cleaning and Transformation using PySpark

This script cleans the raw patient dataset. It removes duplicate records, fills missing age values, converts date columns, casts numeric columns, and creates a new `risk_category` column.

```python
from pyspark.sql.functions import col, when, to_date

df_cleaned = df.dropDuplicates()

avg_age = df_cleaned.selectExpr("avg(age)").collect()[0][0]
df_cleaned = df_cleaned.fillna({"age": int(avg_age)})

df_cleaned = df_cleaned.withColumn("admission_date", to_date(col("admission_date"), "yyyy-MM-dd"))
df_cleaned = df_cleaned.withColumn("discharge_date", to_date(col("discharge_date"), "yyyy-MM-dd"))

df_cleaned = df_cleaned.withColumn("age", col("age").cast("int"))
df_cleaned = df_cleaned.withColumn("blood_pressure", col("blood_pressure").cast("int"))
df_cleaned = df_cleaned.withColumn("cholesterol", col("cholesterol").cast("int"))
df_cleaned = df_cleaned.withColumn("risk_score", col("risk_score").cast("int"))

df_cleaned = df_cleaned.withColumn(
    "risk_category",
    when(col("risk_score") >= 80, "High")
    .when(col("risk_score") >= 50, "Medium")
    .otherwise("Low")
)

display(df_cleaned)
```

Transformations performed:

```text
1. Removed duplicate records.
2. Replaced missing age values with the average age.
3. Converted admission_date and discharge_date to proper date format.
4. Converted age, blood_pressure, cholesterol, and risk_score to integer type.
5. Created a new risk_category column based on risk_score.
```

Risk category logic:

```text
risk_score >= 80: High
risk_score >= 50 and < 80: Medium
risk_score < 50: Low
```

Screenshot:

```text
Step_8_Screenshot_1_Databricks_Cleaned_Data.png
```

---

## Script 3: Data Quality Checks using PySpark

This script performs data quality validation on the source and cleaned data.

```python
source_count = df.count()
cleaned_count = df_cleaned.count()
duplicate_count = source_count - df.dropDuplicates().count()

print("Source Record Count:", source_count)
print("Cleaned Record Count:", cleaned_count)
print("Duplicate Records Removed:", duplicate_count)

print("Null Counts by Column:")
for column_name in df_cleaned.columns:
    null_count = df_cleaned.filter(col(column_name).isNull()).count()
    print(column_name, ":", null_count)

print("Schema:")
df_cleaned.printSchema()
```

Validation checks performed:

```text
1. Source record count check.
2. Cleaned record count check.
3. Duplicate record count check.
4. Null value check for each column.
5. Final schema validation.
```

Expected result:

```text
Source Record Count: 10
Cleaned Record Count: 9
Duplicate Records Removed: 1
```

Screenshot:

```text
Step_9_Screenshot_1_Data_Quality_Checks.png
```

---

## Script 4: Compare Source and Transformed Data

This script compares the source dataset with the transformed dataset.

```python
print("Source Data Preview:")
display(df)

print("Transformed Data Preview:")
display(df_cleaned)
```

Additional column comparison:

```python
source_columns = set(df.columns)
cleaned_columns = set(df_cleaned.columns)

print("Source Columns:", source_columns)
print("Cleaned Columns:", cleaned_columns)
print("New Columns Added:", cleaned_columns - source_columns)
```

Expected new column:

```text
risk_category
```

Screenshot:

```text
Step_9_Screenshot_2_Source_vs_Transformed_Comparison.png
```

---

## Script 5: Save Processed Data to ADLS Processed Container

This script saves the cleaned patient data into the `processed` container in Parquet format.

```python
processed_container_name = "processed"

output_path = f"abfss://{processed_container_name}@{storage_account_name}.dfs.core.windows.net/cleaned_patient_records"

df_cleaned.write.mode("overwrite").parquet(output_path)

print("Processed data saved successfully to ADLS processed container.")
```

Output path:

```text
processed/cleaned_patient_records
```

Screenshot:

```text
Step_10_Screenshot_1_Processed_Data_Saved.png
```

---

## Script 6: Verify Processed Data from ADLS

This script reads the saved Parquet output from the `processed` container to confirm that the data was saved successfully.

```python
processed_df = spark.read.parquet(output_path)

display(processed_df)
```

Screenshot:

```text
Step_10_Screenshot_2_Processed_Data_Verified.png
```

The processed container was also checked in Azure Portal to verify that the `cleaned_patient_records` folder was created.

Screenshot:

```text
Step_10_Screenshot_3_Processed_Container_Output.png
```

---

# Azure SQL Database Setup

---

## Step 8: Create Azure SQL Database

An Azure SQL Database was created to store the final cleaned patient records in a structured table format.

Configuration used:

```text
SQL Server Name: healthcare-sql-server-vishnu123
Database Name: healthcare_etl_db
Authentication Method: SQL Authentication
Admin Username: sqladminuser
```

The final server name should look like:

```text
healthcare-sql-server-vishnu123.database.windows.net
```

Screenshot:

```text
Step_11_Screenshot_1_SQL_Database_Created.png
```

---

## Step 9: Configure SQL Server Networking

SQL Server networking was configured to allow Databricks and the local computer to connect.

Settings used:

```text
Public Network Access: Selected networks
Allow Azure services and resources to access this server: Enabled
Client IPv4 Address: Added
```

This was required because Databricks needs permission to write data into Azure SQL Database.

Screenshot:

```text
Step_12_Screenshot_1_SQL_Networking_Configured.png
```

---

## Script 7: Load Cleaned Data from Databricks to Azure SQL Database

This script writes the cleaned PySpark DataFrame into Azure SQL Database using JDBC.

Important: Do not upload the real SQL password to GitHub. Replace it with a placeholder before submitting.

```python
jdbc_hostname = "healthcare-sql-server-vishnu123.database.windows.net"
jdbc_database = "healthcare_etl_db"
jdbc_port = 1433

jdbc_username = "sqladminuser"
jdbc_password = "PASTE_YOUR_SQL_PASSWORD_HERE"

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

connection_properties = {
    "user": jdbc_username,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df_cleaned.write.jdbc(
    url=jdbc_url,
    table="cleaned_patient_records",
    mode="overwrite",
    properties=connection_properties
)

print("Cleaned patient records loaded into Azure SQL Database successfully.")
```

Explanation:

```text
jdbc_hostname: Azure SQL Server address.
jdbc_database: Target Azure SQL Database name.
jdbc_port: SQL Server default port, 1433.
jdbc_username: SQL admin username.
jdbc_password: SQL admin password.
jdbc_url: JDBC connection string used by Databricks.
connection_properties: Login details and SQL Server JDBC driver.
df_cleaned.write.jdbc(): Writes the cleaned DataFrame into Azure SQL.
mode="overwrite": Creates or replaces the target SQL table.
```

Screenshot:

```text
Step_13_Screenshot_1_Data_Loaded_To_SQL_From_Databricks.png
```

---

# Azure SQL Validation Queries

---

## Query 1: View All Cleaned Patient Records

```sql
SELECT * FROM cleaned_patient_records;
```

Purpose:

```text
This query verifies that the cleaned patient records were successfully loaded into Azure SQL Database.
```

Screenshot:

```text
Step_14_Screenshot_1_SQL_Select_All.png
```

---

## Query 2: Count Patients by Risk Category

```sql
SELECT risk_category, COUNT(*) AS total_patients
FROM cleaned_patient_records
GROUP BY risk_category;
```

Purpose:

```text
This query shows the number of patients in each risk category: High, Medium, and Low.
```

Screenshot:

```text
Step_14_Screenshot_2_SQL_Risk_Category_Query.png
```

---

## Query 3: Count Cases by Diagnosis

```sql
SELECT diagnosis, COUNT(*) AS total_cases
FROM cleaned_patient_records
GROUP BY diagnosis;
```

Purpose:

```text
This query shows the number of patient cases for each diagnosis type.
```

Screenshot:

```text
Step_14_Screenshot_3_SQL_Diagnosis_Query.png
```

---

## Query 4: Check SQL Table Schema Integrity

```sql
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'cleaned_patient_records';
```

Purpose:

```text
This query validates the SQL table schema by checking column names, data types, and nullable settings.
```

Screenshot:

```text
Step_14_Screenshot_4_SQL_Schema_Integrity.png
```

---

## Query 5: Basic Query Performance Check

```sql
SET STATISTICS TIME ON;

SELECT risk_category, COUNT(*) AS total_patients
FROM cleaned_patient_records
GROUP BY risk_category;

SET STATISTICS TIME OFF;
```

Purpose:

```text
This query checks basic SQL query execution performance using STATISTICS TIME.
```

Screenshot:

```text
Step_14_Screenshot_5_SQL_Query_Performance.png
```

---

# Dashboard and Healthcare Insights

Simple healthcare insights were created using Databricks summary queries.

---

## Script 8: Risk Category Summary

```python
risk_summary = df_cleaned.groupBy("risk_category").count()

display(risk_summary)
```

Purpose:

```text
This summary shows how many patients belong to each risk category.
```

Screenshot:

```text
Step_15_Screenshot_1_Risk_Category_Dashboard.png
```

---

## Script 9: Diagnosis Summary

```python
diagnosis_summary = df_cleaned.groupBy("diagnosis").count()

display(diagnosis_summary)
```

Purpose:

```text
This summary shows how many patients are associated with each diagnosis type.
```

Screenshot:

```text
Step_15_Screenshot_2_Diagnosis_Dashboard.png
```

---

# Azure Data Factory Pipeline

---

## Step 10: Create Azure Data Factory

Azure Data Factory was created to demonstrate pipeline orchestration and data movement.

Configuration used:

```text
ADF Name: healthcare-etl-adf
Resource Group: healthcare-etl-rg
Region: Canada Central
```

---

## Step 11: Create Linked Service in ADF

A linked service was created in Azure Data Factory to connect ADF to Azure Data Lake Storage Gen2.

Linked service settings:

```text
Linked Service Type: Azure Data Lake Storage Gen2
Authentication Type: Account Key
Storage Account: healthcareetlstorage
Integration Runtime: AutoResolveIntegrationRuntime
```

Purpose:

```text
The linked service allows Azure Data Factory to access the ADLS Gen2 storage account.
```

---

## Step 12: Create ADF Copy Activity Pipeline

A Copy Activity pipeline was created in Azure Data Factory.

Pipeline name:

```text
healthcare_patient_copy_pipeline
```

Source dataset:

```text
Storage: Azure Data Lake Storage Gen2
Container: raw
File: sample_patient_records.csv
Import schema: From connection/store
```

Sink dataset:

```text
Storage: Azure Data Lake Storage Gen2
Container: processed
File: copied_patient_records.csv
Import schema: None
```

Copy Activity flow:

```text
raw/sample_patient_records.csv
        ↓
ADF Copy Activity
        ↓
processed/copied_patient_records.csv
```

Screenshots:

```text
Step_16_Screenshot_1_ADF_Pipeline_Created.png
Step_16_Screenshot_2_ADF_Debug_Success.png
```

---

# Role of Azure Data Factory in This Project

Azure Data Factory was used as the data pipeline and orchestration tool. In this project, ADF Copy Activity copied the raw patient CSV file from the `raw` container to the `processed` container in Azure Data Lake Storage Gen2.

The actual cleaning and transformation were performed in Azure Databricks using PySpark.

In a real-world healthcare ETL workflow, Azure Data Factory can also be used to schedule the pipeline, trigger Databricks notebooks, and monitor pipeline execution.

---

# Validation and Verification

The project requirements asked for three main validation steps.

---

## 1. Perform Data Quality Checks using PySpark

Completed in Azure Databricks using PySpark.

Checks performed:

```text
Source record count
Cleaned record count
Duplicate record count
Null value check
Schema check
```

Code used:

```python
source_count = df.count()
cleaned_count = df_cleaned.count()
duplicate_count = source_count - df.dropDuplicates().count()

print("Source Record Count:", source_count)
print("Cleaned Record Count:", cleaned_count)
print("Duplicate Records Removed:", duplicate_count)

print("Null Counts by Column:")
for column_name in df_cleaned.columns:
    null_count = df_cleaned.filter(col(column_name).isNull()).count()
    print(column_name, ":", null_count)

print("Schema:")
df_cleaned.printSchema()
```

---

## 2. Compare Transformed Data with Source Records

Completed by comparing source and cleaned record counts.

Expected result:

```text
Source Record Count: 10
Cleaned Record Count: 9
Duplicate Records Removed: 1
```

This proves that the duplicate record was removed during transformation.

Additional comparison was completed by displaying both source and transformed datasets.

```python
print("Source Data Preview:")
display(df)

print("Transformed Data Preview:")
display(df_cleaned)
```

---

## 3. Check Database Schema Integrity and Query Performance

Completed in Azure SQL Database using SQL queries.

Schema integrity query:

```sql
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'cleaned_patient_records';
```

Query performance check:

```sql
SET STATISTICS TIME ON;

SELECT risk_category, COUNT(*) AS total_patients
FROM cleaned_patient_records
GROUP BY risk_category;

SET STATISTICS TIME OFF;
```

---

# Final Deliverables

The project includes the following deliverables:

```text
1. PySpark transformation scripts
2. Processed data stored in Azure SQL Database
3. Data quality validation checks
4. Azure Data Factory pipeline screenshot
5. Databricks notebook execution screenshots
6. Azure SQL query validation screenshots
7. Processed data saved in ADLS Gen2
8. Healthcare insight summaries/dashboard screenshots
9. README documentation
10. GitHub repository with organized folder structure
```

---

# Project Completion Criteria

The project is considered complete because:

```text
1. Raw patient data was uploaded to ADLS Gen2.
2. Azure Data Factory was used to copy the raw data file.
3. Azure Databricks successfully read the raw patient data.
4. PySpark was used to clean and transform the dataset.
5. Duplicate records were removed.
6. Missing age values were handled.
7. Date and numeric columns were converted to correct formats.
8. A new risk_category column was created.
9. Data quality checks were performed using PySpark.
10. Cleaned data was saved to the ADLS processed container.
11. Cleaned data was loaded into Azure SQL Database.
12. SQL queries were used for validation and reporting.
13. Dashboard/summary outputs were created for healthcare insights.
```

---

# Challenges Faced

Some challenges faced during the project included:

```text
1. Configuring Databricks access to Azure Data Lake Storage.
2. Understanding the difference between raw and processed containers.
3. Handling SQL Server firewall settings.
4. Using JDBC to connect Databricks with Azure SQL Database.
5. Creating ADF linked services and datasets.
6. Understanding why sink schema import should be set to None when the output file does not exist yet.
```

---

# Key Learnings

Through this project, the following concepts were learned:

```text
1. How to create Azure Data Lake Storage Gen2.
2. How to upload raw data into ADLS containers.
3. How to connect Azure Databricks with ADLS.
4. How to use PySpark for data cleaning and transformation.
5. How to save processed data in Parquet format.
6. How to connect Databricks to Azure SQL Database using JDBC.
7. How to validate data using PySpark and SQL.
8. How to create a simple Azure Data Factory Copy Activity pipeline.
9. How to organize project files for GitHub submission.
```

---

# Security Note

Access keys and passwords were used during development for learning purposes. However, real credentials should not be uploaded to GitHub.

Before submitting the project, replace sensitive values with placeholders:

```python
storage_account_key = "PASTE_YOUR_STORAGE_ACCOUNT_KEY_HERE"
jdbc_password = "PASTE_YOUR_SQL_PASSWORD_HERE"
```

---


---

# Conclusion

This project successfully demonstrates a basic healthcare ETL pipeline using Azure Databricks, Azure Data Lake Storage Gen2, Azure Data Factory, Azure SQL Database, and PySpark. The raw patient data was ingested, copied through Azure Data Factory, cleaned and transformed using Databricks, validated using PySpark and SQL, saved into the processed container, and loaded into Azure SQL Database for reporting and healthcare insights.

The project provides practical experience in building and validating a cloud-based data engineering pipeline using Azure services.
