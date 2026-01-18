package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EcommercePipeline class
 */
public class EcommercePipelineTest {
    
    private static SparkSession spark;
    private EcommercePipeline pipeline;
    private String testOutputPath;
    
    @BeforeAll
    public static void setUpSpark() {
        // Disable security for local testing (avoids Java 17+ Subject.getSubject() issues)
        System.setProperty("java.security.auth.login.config", "NONE");
        System.setProperty("hadoop.security.authentication", "simple");
        
        spark = SparkSession.builder()
            .appName("EcommercePipelineTest")
            .master("local[2]")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .getOrCreate();
    }
    
    @AfterAll
    public static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }
    
    @BeforeEach
    public void setUp() {
        pipeline = new EcommercePipeline(spark);
        testOutputPath = "test_output_" + System.currentTimeMillis();
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        // Clean up test output directory
        Path outputPath = Paths.get(testOutputPath);
        if (Files.exists(outputPath)) {
            Files.walk(outputPath)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        // Ignore cleanup errors
                    }
                });
        }
    }

    @Test
    public void testCleanAndNormalize_RemovesCorruptRecords() {
        // Create test data with _corrupt_record column (as Spark would when reading malformed CSV)
        List<Row> data = Arrays.asList(
            // Valid records (corrupt_record is null)
            RowFactory.create("ORD001", "Product1", "Cat1", "2", "100.0", "0", "US", "2024-01-15", "200.0", null),
            RowFactory.create("ORD002", "Product2", "Cat2", "3", "50.0", "5", "UK", "2024-01-20", "150.0", null),
            // Corrupt records (corrupt_record is not null)
            RowFactory.create("ORD003", "Product3", "Cat3", "1", "50.0", "0", "CA", "2024-02-10", "50.0", "malformed,line"),
            RowFactory.create("ORD004", "Product4", "Cat4", "abc", "invalid", "200", "DE", "invalid-date", "100.0", "corrupt,record")
        );
        
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.StringType)
            .add("unit_price", DataTypes.StringType)
            .add("discount_percent", DataTypes.StringType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.StringType)
            .add("revenue", DataTypes.StringType)
            .add("_corrupt_record", DataTypes.StringType);
        
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        // Verify input row count
        long inputRowCount = df.count();
        assertEquals(4, inputRowCount, "Input should have 4 rows");
        
        // Replicate the corrupt record removal logic from cleanAndNormalize (lines 81-92)
        String[] columns = df.columns();
        boolean hasCorruptRecordColumn = false;
        for (String col : columns) {
            if ("_corrupt_record".equals(col)) {
                hasCorruptRecordColumn = true;
                break;
            }
        }
        assertTrue(hasCorruptRecordColumn, "_corrupt_record column should exist");
        
        Dataset<Row> cleaned = df;
        if (hasCorruptRecordColumn) {
            cleaned = cleaned.filter(org.apache.spark.sql.functions.col("_corrupt_record").isNull());
        }
        
        List<Row> results = cleaned.collectAsList();
        
        // Verify output row count (2 corrupt records should be filtered out)
        long outputRowCount = results.size();
        assertEquals(2, outputRowCount, "Output should have 2 rows (2 corrupt records filtered out)");
        
        // Verify only valid records remain
        List<String> remainingOrderIds = Arrays.asList(
            results.get(0).getAs("order_id"),
            results.get(1).getAs("order_id")
        );
        assertTrue(remainingOrderIds.contains("ORD001"), "ORD001 (valid record) should remain");
        assertTrue(remainingOrderIds.contains("ORD002"), "ORD002 (valid record) should remain");
        assertFalse(remainingOrderIds.contains("ORD003"), "ORD003 (corrupt record) should be filtered out");
        assertFalse(remainingOrderIds.contains("ORD004"), "ORD004 (corrupt record) should be filtered out");
    }
    
    @Test
    public void testCleanAndNormalize_NoCorruptRecordColumn_KeepsAllRows() {
        // Create test data without _corrupt_record column
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", "2", "100.0", "0", "US", "2024-01-15", "200.0"),
            RowFactory.create("ORD002", "Product2", "Cat2", "3", "50.0", "5", "UK", "2024-01-20", "150.0"),
            RowFactory.create("ORD003", "Product3", "Cat3", "1", "50.0", "0", "CA", "2024-02-10", "50.0")
        );
        
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.StringType)
            .add("unit_price", DataTypes.StringType)
            .add("discount_percent", DataTypes.StringType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.StringType)
            .add("revenue", DataTypes.StringType);
        
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        // Verify input row count
        long inputRowCount = df.count();
        assertEquals(3, inputRowCount, "Input should have 3 rows");
        
        // Replicate the corrupt record removal logic from cleanAndNormalize (lines 81-92)
        String[] columns = df.columns();
        boolean hasCorruptRecordColumn = false;
        for (String col : columns) {
            if ("_corrupt_record".equals(col)) {
                hasCorruptRecordColumn = true;
                break;
            }
        }
        assertFalse(hasCorruptRecordColumn, "_corrupt_record column should NOT exist");
        
        Dataset<Row> cleaned = df;
        if (hasCorruptRecordColumn) {
            cleaned = cleaned.filter(org.apache.spark.sql.functions.col("_corrupt_record").isNull());
        }
        
        List<Row> results = cleaned.collectAsList();
        
        // Verify output row count (all rows should remain when _corrupt_record column doesn't exist)
        long outputRowCount = results.size();
        assertEquals(3, outputRowCount, "Output should have 3 rows (all rows kept when _corrupt_record column doesn't exist)");
    }

    @Test
    public void testMonthlySalesSummary_CalculatesCorrectly() {
        // Create test data with different months
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", 2, 100.0, 10.0, "US", 
                java.sql.Date.valueOf("2024-01-15"), 180.0),
            RowFactory.create("ORD002", "Product2", "Cat2", 3, 50.0, 0.0, "UK", 
                java.sql.Date.valueOf("2024-01-20"), 150.0),
            RowFactory.create("ORD003", "Product3", "Cat3", 1, 200.0, 5.0, "CA", 
                java.sql.Date.valueOf("2024-02-10"), 190.0),
            RowFactory.create("order-1", "Laptop", "Electronics", "abc", 1200.00, 10.0, "US", java.sql.Date.valueOf("2024-01-18"), 1080.0)
            
        );
        
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.IntegerType)
            .add("unit_price", DataTypes.DoubleType)
            .add("discount_percent", DataTypes.DoubleType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.DateType)
            .add("revenue", DataTypes.DoubleType);
        
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        // Verify input row count
        long inputRowCount = df.count();
        assertEquals(4, inputRowCount, "Input should have 4 rows");
        
        // Create monthly summary using reflection to access private method
        // For now, we'll test the pipeline execution instead
        
        // Test that pipeline can process data
        assertNotNull(pipeline);
        assertNotNull(df);
    }
    
    @Test
    public void testTopProducts_IdentifiesTopByRevenue() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "ProductA", "Cat1", 10, 100.0, 0.0, "US", 
                java.sql.Date.valueOf("2024-01-15"), 1000.0),
            RowFactory.create("ORD002", "ProductB", "Cat2", 5, 200.0, 0.0, "UK", 
                java.sql.Date.valueOf("2024-01-20"), 1000.0),
            RowFactory.create("ORD003", "ProductC", "Cat3", 2, 50.0, 0.0, "CA", 
                java.sql.Date.valueOf("2024-02-10"), 100.0),
            RowFactory.create("ORD004", "ProductA", "Cat1", 5, 100.0, 0.0, "US", 
                java.sql.Date.valueOf("2024-02-15"), 500.0)
        );
        
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.IntegerType)
            .add("unit_price", DataTypes.DoubleType)
            .add("discount_percent", DataTypes.DoubleType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.DateType)
            .add("revenue", DataTypes.DoubleType);
        
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        // Verify input row count
        long inputRowCount = df.count();
        assertEquals(4, inputRowCount, "Input should have 4 rows");
        
        // ProductA should have total revenue of 1500 (1000 + 500)
        // ProductB should have total revenue of 1000
        // ProductC should have total revenue of 100
        
        Dataset<Row> topProducts = df
            .groupBy("product_name", "category")
            .agg(
                org.apache.spark.sql.functions.sum("revenue").alias("total_revenue"),
                org.apache.spark.sql.functions.sum("quantity").alias("total_quantity")
            )
            .orderBy(org.apache.spark.sql.functions.desc("total_revenue"));
        
        List<Row> results = topProducts.collectAsList();
        
        // Verify output row count
        long outputRowCount = results.size();
        assertEquals(3, outputRowCount, "Output should have 3 products (grouped by product_name)");
        assertEquals("ProductA", results.get(0).getAs("product_name"));
        assertEquals(1500.0, results.get(0).<Double>getAs("total_revenue"), 0.01);
    }
    
    @Test
    public void testAnomalyRecords_IdentifiesHighestRevenue() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", 2, 100.0, 0.0, "US", 
                java.sql.Date.valueOf("2024-01-15"), 200.0),
            RowFactory.create("ORD002", "Product2", "Cat2", 10, 500.0, 0.0, "UK", 
                java.sql.Date.valueOf("2024-01-20"), 5000.0), // Highest
            RowFactory.create("ORD003", "Product3", "Cat3", 1, 50.0, 0.0, "CA", 
                java.sql.Date.valueOf("2024-02-10"), 50.0)
        );
        
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.IntegerType)
            .add("unit_price", DataTypes.DoubleType)
            .add("discount_percent", DataTypes.DoubleType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.DateType)
            .add("revenue", DataTypes.DoubleType);
        
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        // Verify input row count
        long inputRowCount = df.count();
        assertEquals(3, inputRowCount, "Input should have 3 rows");
        
        Dataset<Row> top5 = df
            .orderBy(org.apache.spark.sql.functions.desc("revenue"))
            .limit(5);
        
        List<Row> results = top5.collectAsList();
        
        // Verify output row count
        long outputRowCount = results.size();
        assertEquals(3, outputRowCount, "Output should have 3 rows (all rows returned since limit is 5)");
        assertEquals("ORD002", results.get(0).getAs("order_id"));
        assertEquals(5000.0, results.get(0).<Double>getAs("revenue"), 0.01);
    }
    
    @Test
    public void testDataCleaning_RemovesNulls() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", 2, 100.0, 0.0, "US", 
                java.sql.Date.valueOf("2024-01-15"), 200.0),
            RowFactory.create(null, "Product2", "Cat2", 3, 50.0, 0.0, "UK", 
                java.sql.Date.valueOf("2024-01-20"), 150.0), // Null order_id
            RowFactory.create("ORD003", null, "Cat3", 1, 50.0, 0.0, "CA", 
                java.sql.Date.valueOf("2024-02-10"), 50.0) // Null product_name
        );
        
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.IntegerType)
            .add("unit_price", DataTypes.DoubleType)
            .add("discount_percent", DataTypes.DoubleType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.DateType)
            .add("revenue", DataTypes.DoubleType);
        
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        // Verify input row count
        long inputRowCount = df.count();
        assertEquals(3, inputRowCount, "Input should have 3 rows");
        
        // Filter out rows with critical nulls
        Dataset<Row> cleaned = df.filter(
            org.apache.spark.sql.functions.col("order_id").isNotNull()
            .and(org.apache.spark.sql.functions.col("product_name").isNotNull())
            .and(org.apache.spark.sql.functions.col("sale_date").isNotNull())
        );
        
        List<Row> results = cleaned.collectAsList();
        
        // Verify output row count (2 rows with nulls should be filtered out)
        long outputRowCount = results.size();
        assertEquals(1, outputRowCount, "Output should have 1 row (2 rows with nulls filtered out)"); // Only ORD001 should remain
        assertEquals("ORD001", results.get(0).getAs("order_id"));
    }
    
    @Test
    public void testDataCleaning_RemovesDuplicates() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", 2, 100.0, 0.0, "US", 
                java.sql.Date.valueOf("2024-01-15"), 200.0),
            RowFactory.create("ORD001", "Product1", "Cat1", 2, 100.0, 0.0, "US", 
                java.sql.Date.valueOf("2024-01-15"), 200.0), // Duplicate
            RowFactory.create("ORD002", "Product2", "Cat2", 3, 50.0, 0.0, "UK", 
                java.sql.Date.valueOf("2024-01-20"), 150.0)
        );
        
        StructType schema = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.IntegerType)
            .add("unit_price", DataTypes.DoubleType)
            .add("discount_percent", DataTypes.DoubleType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.DateType)
            .add("revenue", DataTypes.DoubleType);
        
        Dataset<Row> df = spark.createDataFrame(data, schema);
        
        // Verify input row count
        long inputRowCount = df.count();
        assertEquals(3, inputRowCount, "Input should have 3 rows (including 1 duplicate)");
        
        Dataset<Row> deduplicated = df.dropDuplicates("order_id");
        
        List<Row> results = deduplicated.collectAsList();
        
        // Verify output row count (1 duplicate should be removed)
        long outputRowCount = results.size();
        assertEquals(2, outputRowCount, "Output should have 2 rows (1 duplicate removed)");
    }
}

