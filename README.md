# STEDI Human Balance Analytics

## Project Overview
This project implements a data lakehouse solution on AWS for STEDI Step Trainer sensor data. The solution processes data from IoT devices and mobile applications to create a curated dataset for training machine learning models to detect balance exercises.

## Architecture
The project uses a three-tier data lakehouse architecture:
- **Landing Zone**: Raw data ingestion from S3
- **Trusted Zone**: Sanitized data with privacy filters applied
- **Curated Zone**: Aggregated data ready for machine learning

### Technologies Used
- **AWS S3**: Data storage
- **AWS Glue**: ETL processing and Data Catalog
- **AWS Athena**: SQL queries and data validation
- **Apache Spark**: Data transformation (via Glue)
- **Python**: Glue job scripting

## Data Sources
1. **Customer Records** (956 records)
   - Source: STEDI fulfillment system and website
   - Contains: Customer information and consent preferences

2. **Accelerometer Data** (81,273 records)
   - Source: STEDI mobile application
   - Contains: Motion sensor readings (x, y, z coordinates)

3. **Step Trainer Data** (28,680 records)
   - Source: STEDI Step Trainer IoT device
   - Contains: Distance sensor readings

## Project Structure
```
stedi-lakehouse-project/
├── sql_scripts/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   └── step_trainer_landing.sql
├── glue_jobs/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── step_trainer_trusted.py
│   └── machine_learning_curated.py
├── screenshots/
│   ├── customer_landing.png
│   ├── accelerometer_landing.png
│   ├── step_trainer_landing.png
│   ├── customer_trusted.png
│   ├── accelerometer_trusted.png
│   ├── customer_curated.png
│   ├── step_trainer_trusted.png
│   └── machine_learning_curated.png
└── README.md
```

## Data Pipeline Flow

### Landing Zone
Raw data is ingested into S3 landing folders and cataloged in AWS Glue:
- `customer_landing` → 956 rows
- `accelerometer_landing` → 81,273 rows
- `step_trainer_landing` → 28,680 rows

### Trusted Zone
Data is filtered to include only customers who consented to research:
- `customer_trusted` → 482 rows (filtered for consent)
- `accelerometer_trusted` → 40,981 rows (joined with consenting customers)
- `step_trainer_trusted` → 14,460 rows (matched to verified customers)

### Curated Zone
Final datasets optimized for machine learning:
- `customer_curated` → 482 rows (customers with both consent and accelerometer data)
- `machine_learning_curated` → 43,681 rows (combined step trainer and accelerometer readings)

## Row Count Verification

| Zone | Table | Row Count |
|------|-------|-----------|
| Landing | customer_landing | 956 |
| Landing | accelerometer_landing | 81,273 |
| Landing | step_trainer_landing | 28,680 |
| Trusted | customer_trusted | 482 |
| Trusted | accelerometer_trusted | 40,981 |
| Trusted | step_trainer_trusted | 14,460 |
| Curated | customer_curated | 482 |
| Curated | machine_learning_curated | 43,681 |

## ETL Jobs

### 1. customer_landing_to_trusted.py
- **Purpose**: Filter customers who agreed to share data for research
- **Input**: customer_landing
- **Output**: customer_trusted
- **Logic**: WHERE shareWithResearchAsOfDate IS NOT NULL

### 2. accelerometer_landing_to_trusted.py
- **Purpose**: Filter accelerometer data from consenting customers
- **Input**: customer_trusted + accelerometer_landing
- **Output**: accelerometer_trusted
- **Logic**: INNER JOIN on email

### 3. customer_trusted_to_curated.py
- **Purpose**: Identify customers with both consent and accelerometer data
- **Input**: customer_trusted + accelerometer_trusted
- **Output**: customer_curated
- **Logic**: INNER JOIN with DISTINCT to ensure unique customers

### 4. step_trainer_trusted.py
- **Purpose**: Match step trainer data to verified customers
- **Input**: step_trainer_landing + customer_curated
- **Output**: step_trainer_trusted
- **Logic**: INNER JOIN on serialNumber

### 5. machine_learning_curated.py
- **Purpose**: Combine step trainer and accelerometer data at matching timestamps
- **Input**: step_trainer_trusted + accelerometer_trusted
- **Output**: machine_learning_curated
- **Logic**: INNER JOIN on sensorReadingTime = timeStamp

## Privacy Considerations
- Only processes data from customers who explicitly consented (shareWithResearchAsOfDate IS NOT NULL)
- Filters out all data from non-consenting customers at the Trusted Zone stage
- Ensures serial numbers match real customers to avoid data quality issues

## How to Run

### Prerequisites
- AWS Account with appropriate permissions
- AWS Glue, S3, and Athena access
- IAM role with Glue execution permissions

### Setup Steps
1. Create S3 bucket with required folder structure
2. Upload source data to landing zone folders
3. Run SQL DDL scripts in Athena to create landing zone tables
4. Execute Glue jobs in sequence:
   - customer_landing_to_trusted
   - accelerometer_landing_to_trusted
   - customer_trusted_to_curated
   - step_trainer_trusted
   - machine_learning_curated
5. Verify row counts in Athena

## Query Examples

### Count records in landing zone
```sql
SELECT COUNT(*) FROM customer_landing;
SELECT COUNT(*) FROM accelerometer_landing;
SELECT COUNT(*) FROM step_trainer_landing;
```

### Verify privacy filter
```sql
SELECT COUNT(*) FROM customer_trusted WHERE shareWithResearchAsOfDate IS NULL;
-- Should return 0
```

### View final ML dataset
```sql
SELECT * FROM machine_learning_curated LIMIT 10;
```

## Results
Successfully created a privacy-compliant data lakehouse with 43,681 curated records ready for machine learning model training. All data comes from verified customers who consented to research participation.


## Date
December 2025
