package com.example;

import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the complete EcommercePipeline
 */
@Execution(ExecutionMode.CONCURRENT)
public class EcommercePipelineIntegrationTest {
    
    private static SparkSession spark;
    private EcommercePipeline pipeline;
    private String testInputPath;
    private String testOutputPath;
    
    @BeforeAll
    public static void setUpSpark() {
        // Disable security for local testing (avoids Java 17+ Subject.getSubject() issues)
        System.setProperty("java.security.auth.login.config", "NONE");
        System.setProperty("hadoop.security.authentication", "simple");
        
        spark = SparkSession.builder()
            .appName("EcommercePipelineIntegrationTest")
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
    public void setUp() throws Exception {
        pipeline = new EcommercePipeline(spark);
        testOutputPath = "test_output_" + System.currentTimeMillis();
        
        // Create test input CSV
        testInputPath = "test_input_" + System.currentTimeMillis() + ".csv";
        createTestCSV(testInputPath);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        // Clean up test files
        Path inputPath = Paths.get(testInputPath);
        if (Files.exists(inputPath)) {
            Files.delete(inputPath);
        }
        
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
    
    private void createTestCSV(String filePath) throws Exception {
        StringBuilder csv = new StringBuilder();
        csv.append("order_id,product_name,category,quantity,unit_price,discount_percent,region,sale_date,revenue\n");
        csv.append("ORD001,Laptop,Electronics,2,999.99,10.0,us,2024-01-15,1799.98\n");
        csv.append("ORD002,Phone,Electronics,1,599.99,5.0,uk,2024-01-20,569.99\n");
        csv.append("ORD003,Mouse,Electronics,5,29.99,0,Canada,2024-02-10,149.95\n");
        csv.append("ORD004,Keyboard,Electronics,3,79.99,15.0,USA,2024-02-15,203.97\n");
        csv.append("ORD005,Monitor,Electronics,1,299.99,20.0,Germany,2024-03-01,239.99\n");
        
        Files.write(Paths.get(filePath), csv.toString().getBytes());
    }
    
    @Test
    public void testFullPipeline_GeneratesAllOutputDatasets() throws Exception {
        // Execute the pipeline
        pipeline.execute(testInputPath, testOutputPath);
        
        // Verify output directories exist
        Path monthlySummaryPath = Paths.get(testOutputPath, "monthly_sales_summary");
        Path topProductsPath = Paths.get(testOutputPath, "top_products");
        Path anomalyRecordsPath = Paths.get(testOutputPath, "anomaly_records");
        
        assertTrue(Files.exists(monthlySummaryPath), "monthly_sales_summary directory should exist");
        assertTrue(Files.exists(topProductsPath), "top_products directory should exist");
        assertTrue(Files.exists(anomalyRecordsPath), "anomaly_records directory should exist");
        
        // Verify CSV files exist
        File monthlyDir = monthlySummaryPath.toFile();
        File[] monthlyFiles = monthlyDir.listFiles((dir, name) -> name.endsWith(".csv"));
        assertNotNull(monthlyFiles);
        assertTrue(monthlyFiles.length > 0, "monthly_sales_summary should contain CSV files");
        
        File topProductsDir = topProductsPath.toFile();
        File[] topProductsFiles = topProductsDir.listFiles((dir, name) -> name.endsWith(".csv"));
        assertNotNull(topProductsFiles);
        assertTrue(topProductsFiles.length > 0, "top_products should contain CSV files");
        
        File anomalyDir = anomalyRecordsPath.toFile();
        File[] anomalyFiles = anomalyDir.listFiles((dir, name) -> name.endsWith(".csv"));
        assertNotNull(anomalyFiles);
        assertTrue(anomalyFiles.length > 0, "anomaly_records should contain CSV files");
    }
    
    @Test
    public void testFullPipeline_MonthlySummaryContainsCorrectColumns() throws Exception {
        pipeline.execute(testInputPath, testOutputPath);
        
        // Read the monthly summary output
        Dataset<Row> monthlySummary = spark.read()
            .option("header", "true")
            .csv(testOutputPath + "/monthly_sales_summary");
        
        // Verify columns
        String[] columns = monthlySummary.columns();
        assertTrue(Arrays.asList(columns).contains("month"), "Should contain 'month' column");
        assertTrue(Arrays.asList(columns).contains("total_revenue"), "Should contain 'total_revenue' column");
        assertTrue(Arrays.asList(columns).contains("total_quantity"), "Should contain 'total_quantity' column");
        assertTrue(Arrays.asList(columns).contains("avg_discount_percent"), "Should contain 'avg_discount_percent' column");
        
        // Verify data exists
        assertTrue(monthlySummary.count() > 0, "Monthly summary should have at least one row");
    }
    
    @Test
    public void testFullPipeline_TopProductsContainsCorrectColumns() throws Exception {
        pipeline.execute(testInputPath, testOutputPath);
        
        // Read the top products output
        Dataset<Row> topProducts = spark.read()
            .option("header", "true")
            .csv(testOutputPath + "/top_products");
        
        // Verify columns
        String[] columns = topProducts.columns();
        assertTrue(Arrays.asList(columns).contains("product_name"), "Should contain 'product_name' column");
        assertTrue(Arrays.asList(columns).contains("category"), "Should contain 'category' column");
        assertTrue(Arrays.asList(columns).contains("total_revenue"), "Should contain 'total_revenue' column");
        assertTrue(Arrays.asList(columns).contains("total_quantity"), "Should contain 'total_quantity' column");
        assertTrue(Arrays.asList(columns).contains("rank_type"), "Should contain 'rank_type' column");
        
        // Verify data exists
        assertTrue(topProducts.count() > 0, "Top products should have at least one row");
    }
    
    @Test
    public void testFullPipeline_AnomalyRecordsContainsCorrectColumns() throws Exception {
        pipeline.execute(testInputPath, testOutputPath);
        
        // Read the anomaly records output
        Dataset<Row> anomalies = spark.read()
            .option("header", "true")
            .csv(testOutputPath + "/anomaly_records");
        
        // Verify columns
        String[] columns = anomalies.columns();
        assertTrue(Arrays.asList(columns).contains("order_id"), "Should contain 'order_id' column");
        assertTrue(Arrays.asList(columns).contains("product_name"), "Should contain 'product_name' column");
        assertTrue(Arrays.asList(columns).contains("revenue"), "Should contain 'revenue' column");
        
        // Verify limited to 5 records
        long count = anomalies.count();
        assertTrue(count > 0 && count <= 5, "Anomaly records should have at most 5 rows");
    }
}

