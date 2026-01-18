# E-commerce Data Engineering Pipeline

A scalable Apache Spark-based data engineering pipeline designed to ingest, clean, transform, and analyze large e-commerce sales datasets (approximately 100 million rows).

## Overview

This pipeline processes messy e-commerce sales data with the following capabilities:

- **Ingestion**: Efficiently loads large CSV files (~100M rows) using chunked reading and memory-efficient structures
- **Data Cleaning**: Handles data quality issues including duplicates, typos, invalid values, inconsistent formats, and corrupt records
- **Transformations**: Generates analytical datasets for business insights
- **Comprehensive Testing**: Full test coverage with unit, integration, and performance tests

## Features

### Data Quality Handling

The pipeline addresses various data quality issues:

- **order_id**: Removes duplicates, trims whitespace, filters null/empty values
- **product_name**: Normalizes case, removes special characters
- **category**: Standardizes case, fixes common typos, handles null values
- **quantity**: Converts strings to integers, handles zero/negative values, invalid formats
- **unit_price**: Validates numeric ranges, handles invalid values, currency symbols
- **discount_percent**: Ensures values are within 0-100% range
- **region**: Normalizes variants and fixes typos (e.g., "us" → "United States")
- **sale_date**: Parses multiple date formats (yyyy-MM-dd, MM/dd/yyyy, dd-MM-yyyy, etc.)
- **revenue**: Calculated as `quantity * unit_price * (1 - discount_percent/100)`
- **_corrupt_record**: Filters out malformed records detected during CSV reading

### Corrupt Record Handling

The pipeline automatically detects and filters corrupt records:
- Checks for `_corrupt_record` column (created by Spark when reading malformed CSV files)
- Filters out rows where `_corrupt_record` is not null
- Keeps all records when `_corrupt_record` column doesn't exist

### Analytical Datasets

The pipeline generates three analytical datasets:

1. **monthly_sales_summary**: Aggregated metrics by month
   - Total revenue
   - Total quantity
   - Average discount percent
   - Order count

2. **top_products**: Top 10 products by revenue and by units sold
   - Product name and category
   - Total revenue and quantity
   - Ranking type (by_revenue or by_units)

3. **anomaly_records**: Top 5 highest-revenue records
   - Complete record details for anomaly analysis

## Prerequisites

- Java 17 or higher
- Apache Spark 3.5.0 (or compatible version)
- Maven 3.6+ for building the project

## Project Structure

```
ecommerce-data-pipeline/
├── pom.xml                          # Maven project configuration
├── README.md                        # This file
├── question.txt                     # Original requirements
├── data/                            # Sample data directory
│   ├── sales_sample.csv            # Sample CSV with 100 records
│   └── sales_large.csv             # Larger sample CSV
├── output/                          # Output directory (generated)
│   ├── monthly_sales_summary/      # Monthly aggregation results
│   ├── top_products/               # Top products results
│   └── anomaly_records/            # Anomaly records results
└── src/
    ├── main/
    │   └── java/
    │       └── com/
    │           └── example/
    │               ├── Main.java                    # Entry point
    │               ├── EcommercePipeline.java       # Main pipeline logic
    │               ├── DataCleaner.java             # Data cleaning utilities
    │               └── SampleDataGenerator.java     # Sample data generator utility
    └── test/
        └── java/
            └── com/
                └── example/
                    ├── DataCleanerTest.java                    # Unit tests for DataCleaner
                    ├── EcommercePipelineTest.java              # Unit tests for EcommercePipeline
                    ├── EcommercePipelineIntegrationTest.java   # Integration tests
                    └── EcommercePipelinePerformanceTest.java   # Performance tests
```

## Building the Project

```bash
# Compile the project
mvn clean compile

# Package as JAR (if needed)
mvn clean package
```

## Usage

### Generating Sample Data

A sample CSV file with 100 records is included at `data/sales_sample.csv`. You can also generate more sample data using the `SampleDataGenerator` utility:

```bash
# Generate 1000 sample records
mvn exec:java -Dexec.mainClass="com.example.SampleDataGenerator" \
    -Dexec.args="1000 data/sales_sample.csv"

# Generate 10,000 records (for testing larger datasets)
mvn exec:java -Dexec.mainClass="com.example.SampleDataGenerator" \
    -Dexec.args="10000 data/sales_large.csv"
```

The sample data includes various data quality issues that the pipeline is designed to handle:
- Duplicate order_ids
- Inconsistent product names (different cases, extra spaces)
- Category typos and variants
- Invalid quantity values (negative, zero, non-numeric)
- Invalid unit prices (non-numeric strings)
- Discount percentages out of range (negative, >100%)
- Region variants and typos (us, USA, United States, etc.)
- Multiple date formats (yyyy-MM-dd, MM/dd/yyyy, etc.)
- Invalid date strings
- Corrupt records

### Running the Pipeline

The pipeline accepts two command-line arguments:
1. `inputPath`: Path to the input CSV file
2. `outputPath`: Directory where output datasets will be written

```bash
# Using Maven exec plugin with sample data
mvn exec:java -Dexec.mainClass="com.example.Main" \
    -Dexec.args="data/sales_sample.csv output/"

# Or using java directly (after compilation)
java -cp target/classes com.example.Main data/sales_sample.csv output/
```

### Input Data Format

The input CSV file should have the following columns:
- `order_id` (string, may have duplicates)
- `product_name` (string, may be dirty/inconsistent)
- `category` (string, may have typos)
- `quantity` (string, may contain invalid values)
- `unit_price` (string, may contain invalid ranges)
- `discount_percent` (string, may be out of 0-100 range)
- `region` (string, may have typos/variants)
- `sale_date` (string, multiple formats possible)
- `revenue` (string, will be recalculated)

**Note**: The CSV should have a header row.

### Output

The pipeline generates three output directories under the specified `outputPath`:

```
output/
├── monthly_sales_summary/
│   └── part-00000-*.csv
├── top_products/
│   └── part-00000-*.csv
└── anomaly_records/
    └── part-00000-*.csv
```

Each output is a CSV file with headers, suitable for further analysis or reporting.

## Testing

The project includes comprehensive test coverage with unit, integration, and performance tests. All tests are configured to run in parallel for faster execution.

### Running All Tests

```bash
# Run all tests
mvn test

# Run tests with verbose output
mvn test -X

# Skip tests during build
mvn clean package -DskipTests
```

### Unit Tests

#### DataCleanerTest

Tests all data cleaning methods with comprehensive edge case coverage:

```bash
# Run all DataCleaner tests
mvn test -Dtest=DataCleanerTest

# Run specific test method
mvn test -Dtest=DataCleanerTest#testCleanOrderId_RemovesWhitespace
```

**Test Coverage:**
- `testCleanOrderId_RemovesWhitespace`: Tests order ID cleaning (trimming, duplicates, nulls, empty strings)
- `testCleanProductName_NormalizesCase`: Tests product name normalization (case, special characters, nulls, empty strings)
- `testCleanCategory_HandlesNull`: Tests category cleaning (null handling, case normalization, multiple spaces)
- `testCleanQuantity_HandlesInvalidValues`: Tests quantity validation (nulls, empty strings, negative values, decimals, invalid formats)
- `testCleanUnitPrice_HandlesInvalidValues`: Tests unit price validation (nulls, empty strings, negative values, currency symbols, commas)
- `testCleanDiscountPercent_ClampsRange`: Tests discount validation (range clamping, nulls, empty strings, decimals)
- `testCleanRegion_NormalizesVariants`: Tests region normalization (all mapped regions, nulls, empty strings, capitalization)
- `testCleanSaleDate_HandlesAllFormats`: Tests date parsing (all 6 supported formats, nulls, invalid dates, leap years)
- `testCalculateRevenue_ComputesCorrectly`: Tests revenue calculation (zero values, 100% discount, decimal discounts)

**Note**: Tests run in parallel (`@Execution(ExecutionMode.CONCURRENT)`).

#### EcommercePipelineTest

Tests pipeline logic and transformations:

```bash
# Run all EcommercePipeline tests
mvn test -Dtest=EcommercePipelineTest

# Run specific test method
mvn test -Dtest=EcommercePipelineTest#testCleanAndNormalize_RemovesCorruptRecords
```

**Test Coverage:**
- `testCleanAndNormalize_RemovesCorruptRecords`: Tests corrupt record removal when `_corrupt_record` column exists
- `testCleanAndNormalize_NoCorruptRecordColumn_KeepsAllRows`: Tests behavior when `_corrupt_record` column doesn't exist
- `testMonthlySalesSummary_CalculatesCorrectly`: Tests monthly summary calculation
- `testTopProducts_IdentifiesTopByRevenue`: Tests top products aggregation
- `testAnomalyRecords_IdentifiesHighestRevenue`: Tests anomaly detection
- `testDataCleaning_RemovesNulls`: Tests null filtering
- `testDataCleaning_RemovesDuplicates`: Tests duplicate removal

All tests include assertions for input and output row counts.

### Integration Tests

#### EcommercePipelineIntegrationTest

Tests the complete end-to-end pipeline execution:

```bash
# Run all integration tests
mvn test -Dtest=EcommercePipelineIntegrationTest

# Run specific test
mvn test -Dtest=EcommercePipelineIntegrationTest#testFullPipeline_GeneratesAllOutputDatasets
```

**Test Coverage:**
- `testFullPipeline_GeneratesAllOutputDatasets`: Verifies all output directories and files are created
- `testFullPipeline_MonthlySummaryContainsCorrectColumns`: Validates monthly summary schema
- `testFullPipeline_TopProductsContainsCorrectColumns`: Validates top products schema
- `testFullPipeline_AnomalyRecordsContainsCorrectColumns`: Validates anomaly records schema and row limits

**Note**: Tests run in parallel (`@Execution(ExecutionMode.CONCURRENT)`).

### Performance Tests

#### EcommercePipelinePerformanceTest

Tests pipeline performance with various data sizes:

```bash
# Run all performance tests (includes small dataset test)
mvn test -Dtest=EcommercePipelinePerformanceTest

# Run with large tests enabled (100K and 1M records)
mvn test -Dtest=EcommercePipelinePerformanceTest -DrunLargeTests=true

# Run specific performance test
mvn test -Dtest=EcommercePipelinePerformanceTest#testPerformance_SmallDataset_10K
```

**Test Categories:**

1. **Small Dataset Test (10K records)**: Basic performance validation
   - Measures execution time and throughput
   - Verifies pipeline completes in under 2 minutes

2. **Medium Dataset Test (100K records)**: Scalability testing
   - Tests pipeline with larger datasets
   - Verifies completion in under 10 minutes
   - Requires `-DrunLargeTests=true` flag

3. **Large Dataset Test (1M records)**: Stress testing
   - Tests pipeline with very large datasets
   - Verifies completion in under 1 hour
   - Requires `-DrunLargeTests=true` flag

4. **Component Performance Tests**:
   - `testIngestionPerformance`: Measures data ingestion speed
   - `testCleaningPerformance`: Measures data cleaning throughput
   - `testTransformationPerformance`: Measures analytical dataset generation time
   - `testMemoryEfficiency`: Verifies memory usage stays within limits
   - `testEndToEndPerformance_RealisticData`: Full pipeline performance with realistic data

**Performance Metrics:**
- **Execution Time**: Total time to process the dataset
- **Throughput**: Rows processed per second
- **Memory Usage**: Memory consumed during processing
- **Retention Rate**: Percentage of records retained after cleaning

Example output:
```
=== Performance Test: 10K Records ===
Generating 10000 test records...
Data generation time: 1234 ms
Execution time: 5678 ms
Throughput: 1762.24 rows/second
```

### Running Tests in Parallel

All test classes are configured for parallel execution:
- `DataCleanerTest`: Uses `@Execution(ExecutionMode.CONCURRENT)`
- `EcommercePipelineIntegrationTest`: Uses `@Execution(ExecutionMode.CONCURRENT)`
- Parallel execution is configured in `pom.xml` via Surefire plugin

Tests automatically run in parallel when using `mvn test`.

### Test Reports

Test reports are generated in `target/surefire-reports/`:

```bash
# View test report summary
cat target/surefire-reports/*.txt

# View XML test results
cat target/surefire-reports/TEST-*.xml
```

## Configuration

### Spark Configuration

The pipeline uses optimized Spark settings for large-scale processing:

- Adaptive query execution enabled
- Partition coalescing enabled
- Explicit schema definition for better performance
- Caching of intermediate datasets

To modify Spark settings, edit the `SparkSession.builder()` configuration in `Main.java` or `EcommercePipeline.java`.

### Memory Considerations

For processing ~100M rows, ensure adequate memory allocation:

```bash
# Example: Set Spark driver memory
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=8g
```

Or modify the SparkSession configuration:

```java
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "8g")
```

## Data Cleaning Rules

### Custom Cleaning Logic

The pipeline implements the following cleaning rules:

1. **Corrupt Record Removal**: Filters out rows where `_corrupt_record` column is not null (detected during CSV reading)
2. **Duplicate Removal**: Removes duplicate `order_id` entries (keeps first occurrence)
3. **Null Handling**: Filters out records with critical null values (order_id, product_name, sale_date, quantity, unit_price)
4. **Type Conversion**: Safely converts string fields to appropriate types (integer, double, date)
5. **Range Validation**: Ensures numeric values are within valid ranges
6. **Format Normalization**: Standardizes text fields (case, whitespace, special characters)
7. **Date Parsing**: Handles multiple date formats with fallback logic using regex patterns

### Region Normalization

The pipeline includes a region normalization map for common variants:
- "us", "usa", "united states" → "United States"
- "uk", "united kingdom" → "United Kingdom"
- "can", "canada" → "Canada"
- "australia", "aus" → "Australia"
- "germany", "de" → "Germany"
- "france", "fr" → "France"
- "india", "in" → "India"
- "china", "cn" → "China"
- "japan", "jp" → "Japan"

Unmapped regions are capitalized using standard title case.

### Date Format Support

The pipeline supports parsing multiple date formats:
- `yyyy-MM-dd` (e.g., "2024-01-15")
- `MM/dd/yyyy` (e.g., "01/15/2024")
- `dd-MM-yyyy` (e.g., "15-01-2024")
- `yyyy/MM/dd` (e.g., "2024/01/15")
- `dd/MM/yyyy` (e.g., "15/01/2024")
- `MM-dd-yyyy` (e.g., "01-15-2024")

Additionally, regex fallback patterns handle dates with time components or slight variations.

## Performance Optimization

The pipeline is optimized for large-scale data processing:

- **Schema Definition**: Explicit schema prevents schema inference overhead
- **Caching**: Intermediate datasets are cached for reuse
- **Partitioning**: Uses Spark's adaptive partitioning
- **Coalescing**: Reduces output files to single partition per dataset
- **Parallel Test Execution**: Tests run in parallel for faster feedback

## Example Test Data Structure

```csv
order_id,product_name,category,quantity,unit_price,discount_percent,region,sale_date,revenue
ORD001,Laptop,Electronics,2,999.99,10,us,2024-01-15,1799.98
ORD002,Phone,Electronics,1,599.99,5,uk,01/20/2024,569.99
```

## Troubleshooting

### Common Issues

1. **Out of Memory**: Increase Spark driver/executor memory
2. **Date Parsing Errors**: Check date format in input data
3. **Null Pointer Exceptions**: Ensure input CSV has proper headers
4. **Test Failures**: Ensure Java 17+ is being used and all Maven dependencies are resolved

### Logging

Spark logs are written to the console. For more detailed logging, configure log4j properties.

## License

This project is provided as-is for educational and demonstration purposes.