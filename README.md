# Automotive Manufacturing Data Lakehouse

Data platform for automotive manufacturing analytics built on Azure Databricks using Medallion Architecture.

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=delta&logoColor=white)](https://delta.io/)
[![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/)

---

## Overview

This project implements a data lakehouse for automotive manufacturing operations, processing data from vehicle production, supplier logistics, dealer sales, service orders, warranty claims, and customer records.

The pipeline follows Medallion Architecture (Bronze → Silver → Gold) and includes metadata-driven ingestion, transformations, multiple load patterns, and automated data quality validation.

**Tech Stack:**
- Azure Databricks, PySpark 3.4+
- Delta Lake (ACID transactions, time travel)
- Unity Catalog (governance and lineage)

**Scope:**
- 13 source tables (~320K records)
- 15 Silver tables (cleaned and transformed)
- 18 Gold tables (9 dimensions + 9 facts)
- 40+ data quality validation rules

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│              SOURCE DATA (13 Tables)                      │
│  CSV (5) | Parquet (7) | JSON (1)                        │
└──────────────────────┬───────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────┐
│              BRONZE LAYER (Raw)                           │
│  • Multi-format ingestion                                 │
│  • Schema inference                                        │
│  • Audit logging                                           │
└──────────────────────┬───────────────────────────────────┘
                       │ ✓ Data Quality
                       ▼
┌──────────────────────────────────────────────────────────┐
│              SILVER LAYER (Cleaned)                       │
│  • 15 transformed tables                                   │
│  • SCD Type 1 & Type 2                                     │
│  • Incremental loading                                     │
└──────────────────────┬───────────────────────────────────┘
                       │ ✓ Data Quality
                       ▼
┌──────────────────────────────────────────────────────────┐
│              GOLD LAYER (Analytics)                       │
│  • Star schema (9 dims + 9 facts)                         │
│  • Currency normalization                                  │
│  • BI-optimized                                            │
└──────────────────────────────────────────────────────────┘
```

---

## Bronze Layer

**Purpose:** Ingest raw data from multiple sources with minimal transformation.

**Source Tables (13):**

| Table | Format | Records | Description |
|-------|--------|---------|-------------|
| bronze_customer_master | CSV | 150,000 | Customer demographics and contact info |
| bronze_production_events | Parquet | ~80,000 | Vehicle production events by plant |
| bronze_service_logs | JSON | ~30,000 | Service orders and maintenance records |
| bronze_dealer_sales | Parquet | ~25,000 | Sales transactions by dealer |
| bronze_dealer_inventory | Parquet | ~20,000 | Dealer inventory snapshots |
| bronze_supplier_delivery_schedule | Parquet | ~8,000 | Supplier delivery schedules |
| bronze_warranty_claims | Parquet | ~5,000 | Warranty claims |
| bronze_currency_rates | Parquet | ~2,000 | Exchange rate history |
| bronze_supplier_parts | Parquet | ~500 | Parts catalog |
| bronze_supplier_master | CSV | 300 | Supplier master data |
| bronze_dealer_master | CSV | 46 | Dealer network |
| bronze_country_city_mapping | CSV | 28 | Geographic reference |
| bronze_plant_master | CSV | 10 | Manufacturing plants |

**Features:**
- Multi-format support (CSV, Parquet, JSON)
- Schema inference using Pandas → PyArrow → Spark
- Parameterized execution (catalog, schema, path, table selection)
- Audit logging for success/failure tracking
- Delta Lake for ACID compliance

---

## Silver Layer

**Purpose:** Clean, standardize, and apply business logic to raw data.

**Transformation Approach:**

The Silver layer uses a configuration-driven framework where transformations are defined in JSON files rather than hard-coded in notebooks. This makes it easier to add new tables without writing custom code each time.

**Configuration Example:**
```json
{
  "source_table": "bronze.customer_master",
  "target_table": "silver.silver_customer",
  "read_mode": "incremental",
  "watermark_column": "updated_ts",
  "load_type": "scd1",
  "merge_keys": ["customer_id"],
  "column_mapping": {
    "customer_id": "customer_id",
    "full_name": "concat(first_name, ' ', last_name)",
    "age": "year(current_date()) - year(date_of_birth)"
  }
}
```

**Supported Load Patterns:**

| Pattern | Description | Use Case |
|---------|-------------|----------|
| Append | Insert new records | Transaction logs, events |
| Overwrite | Full refresh | Reference data |
| SCD Type 1 | Update existing records | Current-state data |
| SCD Type 2 | Historical tracking | Audit trails |

**Supported Load Types:**
- **Full:** Process entire source table
- **Incremental:** Process only new/changed records using watermarking

**Silver Tables (15):**

| Table | Load Pattern | Description |
|-------|--------------|-------------|
| silver_dealer | SCD Type 2 | Dealer network with historical tracking |
| silver_supplier | SCD Type 2 | Supplier master with change history |
| silver_manufacturing_plant | SCD Type 2 | Plant data with historical changes |
| silver_customer | SCD Type 1 | Current customer state |
| silver_vehicle_master | SCD Type 1 | Vehicle catalog |
| silver_supplier_parts | SCD Type 1 | Parts catalog |
| silver_geography | Overwrite | Geographic reference |
| silver_currency | Append | Exchange rates |
| silver_vehicle_production | Append | Production events |
| silver_vehicle_sales | Append | Sales transactions |
| silver_service_orders | Append | Service records |
| silver_warranty_claims | Append | Warranty claims |
| silver_inventory_snapshot | Append | Inventory snapshots |
| silver_part_delivery | Append | Delivery events |
| silver_service_center | SCD Type 1 | Service centers |

**SCD Type 2 Example:**

When a dealer's address changes, the framework:
1. Expires the old record (sets `is_current = false`, adds `end_date`)
2. Inserts a new record (sets `is_current = true`, adds `start_date`)

This enables point-in-time analysis, like "Which dealers were active in Q3 2023?"

**Change Detection:**
- MD5 checksum calculated on tracked columns
- Only records with changed checksums trigger SCD logic

**Incremental Loading:**
- Reads `max(watermark_column)` from target table
- Filters source to only records where `watermark_column > last_value`
- Processes delta instead of full dataset

---

## Gold Layer

**Purpose:** Create analytics-ready dimensional model optimized for BI tools.

**Design:** Star schema with fact and dimension tables.

**Dimensions (9):**
- `dim_geography` - Countries, cities, currency codes
- `dim_customer` - Customer demographics with age groups
- `dim_dealer` - Dealer network hierarchy
- `dim_supplier` - Supplier information
- `dim_vehicle` - Vehicle models and specifications
- `dim_part` - Parts catalog
- `dim_manufacturing_plant` - Plant locations and capacity
- `dim_service_center` - Service center network
- `dim_date` - Calendar dimension (2015-2035)

**Facts (9):**
- `fact_vehicle_production` - Production events (plant, vehicle, date, quantity)
- `fact_vehicle_sales` - Sales transactions with currency conversion
- `fact_service_orders` - Service history
- `fact_warranty_claims` - Warranty events
- `fact_inventory_snapshot` - Dealer inventory levels
- `fact_parts_procurement` - Supplier deliveries
- `fact_vehicle_logistics` - Shipping metrics
- `fact_financial_revenue` - Revenue aggregations
- `fact_market_demand_forecast` - Demand analytics

**Key Features:**
- Surrogate keys for all dimensions
- Currency normalization to USD
- Pre-aggregated metrics for performance
- SCD Type 2 joins for historical accuracy

**Example Query:**
```sql
-- Sales by dealer and vehicle model in Q1 2024
SELECT 
    dd.dealer_name,
    dv.model,
    SUM(fs.gross_sales_usd) as total_sales
FROM fact_vehicle_sales fs
JOIN dim_dealer dd ON fs.dealer_sk = dd.dealer_sk
JOIN dim_vehicle dv ON fs.vehicle_sk = dv.vehicle_sk
JOIN dim_date dt ON fs.date_sk = dt.date_sk
WHERE dt.quarter = 'Q1' AND dt.year = 2024
GROUP BY dd.dealer_name, dv.model;
```

---

## Data Quality Framework

**Purpose:** Validate data integrity across all layers before downstream consumption.

**Architecture:**
1. **Rule Master Table:** Stores validation rules as metadata
2. **Execution Engine:** PySpark function that evaluates rules dynamically
3. **Execution Log:** Tracks all validation results for observability

**Coverage:** 40+ rules across 5 dimensions

| Dimension | Example | Threshold | Severity |
|-----------|---------|-----------|----------|
| Completeness | `service_order_id IS NOT NULL` | 100% | FAIL |
| Validity | `service_cost >= 0 AND <= 100000` | 99.5% | FAIL |
| Accuracy | `NOT (warranty_flag = true AND service_cost > 0)` | 99.9% | FAIL |
| Consistency | `vin IN (SELECT vin FROM production)` | 99.5% | FAIL |
| Uniqueness | `sales_id` must be unique | 100% | FAIL |

**Execution:**
- Runs inline after each layer transformation
- Bronze DQ → Silver transformation → Silver DQ → Gold transformation → Gold DQ
- Critical failures (FAIL severity) stop the pipeline
- Warnings (WARN severity) are logged but don't stop execution

**Example Rule:**
```python
{
  "rule_id": "DQ_S_006",
  "table": "silver_service_orders",
  "column": "vin",
  "dimension": "Consistency",
  "rule_type": "REF",
  "expression": "vin IN (SELECT vin FROM silver_vehicle_production)",
  "threshold": 99.5,
  "severity": "FAIL"
}
```

If less than 99.5% of VINs in service orders exist in the production table, the pipeline stops to prevent orphaned records.

---

## Technical Implementation

### **Bronze Ingestion**

Multi-format ingestion with error handling:

```python
TABLE_FILE_MAP = {
    "bronze_supplier_master": ("supplier_master.csv", "csv"),
    "bronze_dealer_sales": ("dealer_sales.parquet", "parquet"),
    "bronze_service_logs": ("service_logs.json", "json"),
    # ... 10 more tables
}

for table_name, (file, format) in TABLE_FILE_MAP.items():
    try:
        df = load_file(f"{BASE_PATH}/{file}", format)
        df = df.withColumn("ingestion_ts", current_timestamp())
        df.write.format("delta").mode("append").saveAsTable(table)
        log_to_audit(table_name, "SUCCESS")
    except Exception as e:
        log_to_audit(table_name, "FAILURE", str(e))
```

### **Silver Transformation**

Configuration-driven approach:

```python
# Read config
config = spark.read.json(config_path).collect()[0].asDict()

# Read source (full or incremental)
if config["read_mode"] == "incremental":
    last_value = get_last_processed_value(target, watermark_col)
    src_df = spark.table(source).filter(col(watermark_col) > last_value)
else:
    src_df = spark.table(source)

# Apply column mappings
mapped_df = src_df.select(*[
    expr(mapping).alias(col) 
    for col, mapping in config["column_mapping"].items()
])

# Generate checksum
mapped_df = mapped_df.withColumn("checksum", md5(concat_ws("|", *cols)))

# Execute load pattern (append/overwrite/scd1/scd2)
execute_load_pattern(mapped_df, config["load_type"])
```

### **SCD Type 2 Logic**

```python
# 1. Identify changed records
changed = new_data.join(current_records, "dealer_id", "left") \
    .filter(col("new.checksum") != col("curr.checksum"))

# 2. Expire old records
delta_table.update(
    condition=f"dealer_id IN ({changed_ids}) AND is_current = true",
    set={"is_current": "false", "end_date": "current_date()"}
)

# 3. Insert new current records
changed.withColumn("is_current", lit(True)) \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None)) \
    .write.mode("append").saveAsTable(target)
```

---

## Repository Structure

```
/Data pipeline code/
  ├── Load_bronze.dbc              # Bronze layer ingestion
  ├── Silver_Table_Definitions.dbc # Silver table schemas
  ├── Load_silver_table.dbc        # Silver transformations
  ├── Load dim tables_gold.dbc     # Gold dimension tables
  ├── Load Fact Tables_gold.dbc    # Gold fact tables
  └── Data quality checks.dbc      # DQ framework

/data/
  ├── bronze_*.csv                 # CSV source files
  ├── bronze_*.parquet             # Parquet source files
  └── bronze_*.json                # JSON source files

--Silver Table Definations
--Mapping files silver layer
```

---

## Key Features

**Medallion Architecture:**
- Clear separation of raw, cleaned, and analytics layers
- Incremental processing for efficiency
- Delta Lake for ACID compliance

**Flexible Loading:**
- 4 load patterns (Append, Overwrite, SCD Type 1, SCD Type 2)
- 2 load types (Full, Incremental)
- Configuration-driven transformations

**Data Quality:**
- Automated validation across 40+ rules
- Inline execution after each layer
- Metadata-driven rule management

**Dimensional Modeling:**
- Star schema optimized for BI
- Surrogate keys for integrity
- Currency normalization
- Historical tracking via SCD Type 2

**Production Patterns:**
- Error handling and audit logging
- Parameterized execution
- Checksum-based change detection
- Watermarking for incremental loads

---

## What This Demonstrates

**Data Engineering:**
- ETL pipeline design
- Medallion Architecture implementation
- Slowly changing dimensions (Type 1 & 2)
- Dimensional modeling (star schema)
- Incremental data processing

**Azure Databricks:**
- PySpark for distributed processing
- Delta Lake for transactional integrity
- Unity Catalog for governance
- Notebook parameterization

**Best Practices:**
- Configuration over code
- Automated data quality
- Error handling and observability
- Scalable design patterns

---

## Contact

**Vanditha V B**  
Data Engineer | Databricks Certified Data Engineer Associate

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/vandithab)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=flat&logo=github&logoColor=white)](https://github.com/vandithavb)

---

