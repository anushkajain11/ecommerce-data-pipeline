package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

/**
 * Main pipeline class for e-commerce sales data processing
 * Handles ingestion, cleaning, normalization, and transformation of ~100M rows
 */
public class EcommercePipeline {
    
    private SparkSession spark;
    private DataCleaner dataCleaner;
    
    public EcommercePipeline(SparkSession spark) {
        this.spark = spark;
        this.dataCleaner = new DataCleaner();
    }
    
    /**
     * Main pipeline execution method
     */
    public void execute(String inputPath, String outputPath) {
        System.out.println("Starting E-commerce Data Pipeline...");
        
        // Step 1: Ingestion
        System.out.println("Step 1: Ingesting data from " + inputPath);
        Dataset<Row> rawData = ingestData(inputPath);
        System.out.println("Ingested " + rawData.count() + " rows");
        
        // Step 2: Cleaning & Normalization
        System.out.println("Step 2: Cleaning and normalizing data...");
        Dataset<Row> cleanedData = cleanAndNormalize(rawData);
        System.out.println("Cleaned data: " + cleanedData.count() + " rows");
        
        // Step 3: Transformations
        System.out.println("Step 3: Generating analytical datasets...");
        generateAnalyticalDatasets(cleanedData, outputPath);
        
        System.out.println("Pipeline execution completed successfully!");
    }
    
    /**
     * Step 1: Ingestion - Load data with chunked reading and memory-efficient structures
     */
    private Dataset<Row> ingestData(String inputPath) {
        // Define schema for better performance and type safety
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType, true)
            .add("product_name", DataTypes.StringType, true)
            .add("category", DataTypes.StringType, true)
            .add("quantity", DataTypes.StringType, true)
            .add("unit_price", DataTypes.StringType, true)
            .add("discount_percent", DataTypes.StringType, true)
            .add("region", DataTypes.StringType, true)
            .add("sale_date", DataTypes.StringType, true)
            .add("revenue", DataTypes.StringType, true);
        
        // Read CSV with optimized settings for large files
        Dataset<Row> df = spark.read()
            .option("header", "true")
            .option("inferSchema", "false") // Use explicit schema for performance
            .option("mode", "PERMISSIVE") // Handle malformed records
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .csv(inputPath);
        
        // Cache for reuse in cleaning step
        df.cache();
        
        return df;
    }
    
    /**
     * Step 2: Cleaning & Normalization
     */
    private Dataset<Row> cleanAndNormalize(Dataset<Row> rawData) {
        Dataset<Row> cleaned = rawData;
        
        // Remove corrupt records (if the column exists)
        String[] columns = cleaned.columns();
        boolean hasCorruptRecordColumn = false;
        for (String col : columns) {
            if ("_corrupt_record".equals(col)) {
                hasCorruptRecordColumn = true;
                break;
            }
        }
        if (hasCorruptRecordColumn) {
            cleaned = cleaned.filter(col("_corrupt_record").isNull());
        }
        
        // Apply cleaning rules
        cleaned = dataCleaner.cleanOrderId(cleaned);
        cleaned = dataCleaner.cleanProductName(cleaned);
        cleaned = dataCleaner.cleanCategory(cleaned);
        cleaned = dataCleaner.cleanQuantity(cleaned);
        cleaned = dataCleaner.cleanUnitPrice(cleaned);
        cleaned = dataCleaner.cleanDiscountPercent(cleaned);
        cleaned = dataCleaner.cleanRegion(cleaned);
        cleaned = dataCleaner.cleanSaleDate(cleaned);
        cleaned = dataCleaner.calculateRevenue(cleaned);
        
        // Remove duplicates based on order_id (keeping first occurrence)
        cleaned = cleaned.dropDuplicates("order_id");
        
        // Filter out rows with critical null values
        cleaned = cleaned.filter(
            col("order_id").isNotNull()
            .and(col("product_name").isNotNull())
            .and(col("sale_date").isNotNull())
            .and(col("quantity").isNotNull())
            .and(col("unit_price").isNotNull())
        );
        
        return cleaned;
    }
    
    /**
     * Execute pipeline and return cleaned data (for testing purposes)
     */
    public Dataset<Row> executeAndReturnCleanedData(String inputPath) {
        Dataset<Row> rawData = ingestData(inputPath);
        return cleanAndNormalize(rawData);
    }
    
    /**
     * Generate analytical datasets from cleaned data (public for testing)
     */
    public void generateAnalyticalDatasets(Dataset<Row> cleanedData, String outputPath) {
        // 1. Monthly Sales Summary
        Dataset<Row> monthlySummary = createMonthlySalesSummary(cleanedData);
        monthlySummary.coalesce(1)
            .write()
            .mode("overwrite")
            .option("header", "true")
            .csv(outputPath + "/monthly_sales_summary");
        System.out.println("Generated monthly_sales_summary");
        
        // 2. Top Products
        Dataset<Row> topProducts = createTopProducts(cleanedData);
        topProducts.coalesce(1)
            .write()
            .mode("overwrite")
            .option("header", "true")
            .csv(outputPath + "/top_products");
        System.out.println("Generated top_products");
        
        // 3. Anomaly Records
        Dataset<Row> anomalies = createAnomalyRecords(cleanedData);
        anomalies.coalesce(1)
            .write()
            .mode("overwrite")
            .option("header", "true")
            .csv(outputPath + "/anomaly_records");
        System.out.println("Generated anomaly_records");
    }
    
    /**
     * Create monthly sales summary: revenue, quantity, avg discount by month
     */
    private Dataset<Row> createMonthlySalesSummary(Dataset<Row> data) {
        return data
            .withColumn("year_month", date_format(col("sale_date"), "yyyy-MM"))
            .groupBy("year_month")
            .agg(
                sum("revenue").alias("total_revenue"),
                sum("quantity").alias("total_quantity"),
                avg("discount_percent").alias("avg_discount_percent"),
                count("order_id").alias("order_count")
            )
            .orderBy("year_month")
            .select(
                col("year_month").alias("month"),
                col("total_revenue"),
                col("total_quantity"),
                col("avg_discount_percent"),
                col("order_count")
            );
    }
    
    /**
     * Create top 10 products by revenue and units
     */
    private Dataset<Row> createTopProducts(Dataset<Row> data) {
        Dataset<Row> byRevenue = data
            .groupBy("product_name", "category")
            .agg(
                sum("revenue").alias("total_revenue"),
                sum("quantity").alias("total_quantity")
            )
            .orderBy(desc("total_revenue"))
            .limit(10)
            .withColumn("rank_type", lit("by_revenue"));
        
        Dataset<Row> byUnits = data
            .groupBy("product_name", "category")
            .agg(
                sum("revenue").alias("total_revenue"),
                sum("quantity").alias("total_quantity")
            )
            .orderBy(desc("total_quantity"))
            .limit(10)
            .withColumn("rank_type", lit("by_units"));
        
        return byRevenue.union(byUnits)
            .select(
                col("product_name"),
                col("category"),
                col("total_revenue"),
                col("total_quantity"),
                col("rank_type")
            )
            .orderBy(desc("rank_type"), desc("total_revenue"));
    }
    
    /**
     * Create top 5 highest-revenue records (anomalies)
     */
    private Dataset<Row> createAnomalyRecords(Dataset<Row> data) {
        return data
            .orderBy(desc("revenue"))
            .limit(5)
            .select(
                col("order_id"),
                col("product_name"),
                col("category"),
                col("quantity"),
                col("unit_price"),
                col("discount_percent"),
                col("region"),
                col("sale_date"),
                col("revenue")
            );
    }
}

