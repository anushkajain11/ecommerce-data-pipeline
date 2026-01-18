package com.example;

import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance tests for EcommercePipeline
 * Tests execution time, throughput, and scalability with different data sizes
 */
public class EcommercePipelinePerformanceTest {
    
    private static SparkSession spark;
    private EcommercePipeline pipeline;
    private String testInputPath;
    private String testOutputPath;
    
    @BeforeAll
    public static void setUpSpark() {
        // Disable security for local testing (avoids Java 17+ Subject.getSubject() issues)
        // MUST be set before SparkSession.builder() is called
        System.setProperty("java.security.auth.login.config", "NONE");
        System.setProperty("hadoop.security.authentication", "simple");
        
        spark = SparkSession.builder()
            .appName("EcommercePipelinePerformanceTest")
            .master("local[2]")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
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
        testOutputPath = "test_perf_output_" + System.currentTimeMillis();
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        // Clean up test files
        if (testInputPath != null) {
            Path inputPath = Paths.get(testInputPath);
            if (Files.exists(inputPath)) {
                Files.delete(inputPath);
            }
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
    
    /**
     * Performance test with small dataset (10K rows)
     */
    @Test
    @Tag("performance")
    public void testPerformance_SmallDataset_10K() throws Exception {
        int numRecords = 10_000;
        testInputPath = "test_input_perf_10k_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== Performance Test: 10K Records ===");
        generateTestData(numRecords, testInputPath);
        
        long startTime = System.nanoTime();
        pipeline.execute(testInputPath, testOutputPath);
        long endTime = System.nanoTime();
        
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (double) numRecords / durationMs * 1000; // rows per second
        
        System.out.println("Execution time: " + durationMs + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        
        // Verify output was generated
        assertTrue(Files.exists(Paths.get(testOutputPath, "monthly_sales_summary")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "top_products")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "anomaly_records")));
        
        // Performance assertion: Should complete in reasonable time (adjust threshold as needed)
        assertTrue(durationMs < 120_000, "10K records should process in under 2 minutes");
    }
    
    /**
     * Performance test with medium dataset (100K rows)
     */
    @Test
    @Tag("performance")
    @EnabledIfSystemProperty(named = "runLargeTests", matches = "true")
    public void testPerformance_MediumDataset_100K() throws Exception {
        int numRecords = 100_000;
        testInputPath = "test_input_perf_100k_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== Performance Test: 100K Records ===");
        generateTestData(numRecords, testInputPath);
        
        long startTime = System.nanoTime();
        pipeline.execute(testInputPath, testOutputPath);
        long endTime = System.nanoTime();
        
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (double) numRecords / durationMs * 1000; // rows per second
        
        System.out.println("Execution time: " + durationMs + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        
        // Verify output was generated
        assertTrue(Files.exists(Paths.get(testOutputPath, "monthly_sales_summary")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "top_products")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "anomaly_records")));
        
        // Performance assertion: Should scale reasonably
        assertTrue(durationMs < 600_000, "100K records should process in under 10 minutes");
    }
    
    /**
     * Performance test with large dataset (1M rows)
     * Only runs if system property runLargeTests=true is set
     */
    @Test
    @Tag("performance")
    @Tag("large")
    @EnabledIfSystemProperty(named = "runLargeTests", matches = "true")
    public void testPerformance_LargeDataset_1M() throws Exception {
        int numRecords = 100_000_000;
        testInputPath = "test_input_perf_1m_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== Performance Test: 1M Records ===");
        generateTestData(numRecords, testInputPath);
        
        long startTime = System.nanoTime();
        pipeline.execute(testInputPath, testOutputPath);
        long endTime = System.nanoTime();
        
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (double) numRecords / durationMs * 1000; // rows per second
        
        System.out.println("Execution time: " + durationMs + " ms (" + (durationMs / 1000.0) + " seconds)");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        
        // Verify output was generated
        assertTrue(Files.exists(Paths.get(testOutputPath, "monthly_sales_summary")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "top_products")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "anomaly_records")));
        
        // Performance assertion: Should handle large datasets
        assertTrue(durationMs < 3_600_000, "1M records should process in under 1 hour");
    }
    
    /**
     * Test ingestion performance separately
     */
    @Test
    @Tag("performance")
    public void testIngestionPerformance() throws Exception {
        int numRecords = 50_000;
        testInputPath = "test_input_ingest_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== Ingestion Performance Test: 50K Records ===");
        generateTestData(numRecords, testInputPath);
        
        long startTime = System.nanoTime();
        Dataset<Row> rawData = spark.read()
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(testInputPath);
        
        long count = rawData.count();
        long endTime = System.nanoTime();
        
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (double) count / durationMs * 1000;
        
        System.out.println("Ingestion time: " + durationMs + " ms");
        System.out.println("Records ingested: " + count);
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        
        assertEquals(numRecords, count, "Should ingest all records");
        assertTrue(durationMs < 60_000, "Ingestion should complete in under 1 minute for 50K records");
    }
    
    /**
     * Test data cleaning performance
     */
    @Test
    @Tag("performance")
    public void testCleaningPerformance() throws Exception {
        int numRecords = 50_000;
        testInputPath = "test_input_clean_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== Cleaning Performance Test: 50K Records ===");
        generateTestData(numRecords, testInputPath);
        
        // Ingest data
        Dataset<Row> rawData = spark.read()
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(testInputPath);
        
        rawData.cache();
        long rawCount = rawData.count();
        
        // Measure cleaning time
        long startTime = System.nanoTime();
        Dataset<Row> cleanedData = pipeline.executeAndReturnCleanedData(testInputPath);
        long cleanedCount = cleanedData.count();
        long endTime = System.nanoTime();
        
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (double) rawCount / durationMs * 1000;
        double retentionRate = (double) cleanedCount / rawCount * 100;
        
        System.out.println("Cleaning time: " + durationMs + " ms");
        System.out.println("Raw records: " + rawCount);
        System.out.println("Cleaned records: " + cleanedCount);
        System.out.println("Retention rate: " + String.format("%.2f", retentionRate) + "%");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        
        assertTrue(cleanedCount <= rawCount, "Cleaned count should be <= raw count");
        assertTrue(durationMs < 120_000, "Cleaning should complete in under 2 minutes for 50K records");
    }
    
    /**
     * Test transformation performance (analytical dataset generation)
     */
    @Test
    @Tag("performance")
    public void testTransformationPerformance() throws Exception {
        int numRecords = 50_000;
        testInputPath = "test_input_transform_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== Transformation Performance Test: 50K Records ===");
        generateTestData(numRecords, testInputPath);
        
        // Execute pipeline to get cleaned data
        Dataset<Row> cleanedData = pipeline.executeAndReturnCleanedData(testInputPath);
        cleanedData.cache();
        long cleanedCount = cleanedData.count();
        
        // Measure transformation time
        long startTime = System.nanoTime();
        pipeline.generateAnalyticalDatasets(cleanedData, testOutputPath);
        long endTime = System.nanoTime();
        
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (double) cleanedCount / durationMs * 1000;
        
        System.out.println("Transformation time: " + durationMs + " ms");
        System.out.println("Records processed: " + cleanedCount);
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        
        // Verify outputs
        assertTrue(Files.exists(Paths.get(testOutputPath, "monthly_sales_summary")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "top_products")));
        assertTrue(Files.exists(Paths.get(testOutputPath, "anomaly_records")));
        
        assertTrue(durationMs < 60_000, "Transformation should complete in under 1 minute for 50K records");
    }
    
    /**
     * Test scalability with different Spark executor configurations
     */
    @Test
    @Tag("performance")
    @Disabled("Requires Spark session restart - run manually with different configs")
    public void testScalability_DifferentExecutorConfigs() {
        // This test demonstrates how to test with different Spark configs
        // In practice, you would restart Spark with different executor counts
        System.out.println("\n=== Scalability Test ===");
        System.out.println("Current Spark config: " + spark.conf().get("spark.master"));
        System.out.println("To test scalability, run tests with different Spark configurations:");
        System.out.println("  - local[2] (2 cores)");
        System.out.println("  - local[4] (4 cores)");
        System.out.println("  - local[8] (8 cores)");
    }
    
    /**
     * Test memory efficiency - verify pipeline doesn't run out of memory
     */
    @Test
    @Tag("performance")
    public void testMemoryEfficiency() throws Exception {
        int numRecords = 100_000;
        testInputPath = "test_input_memory_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== Memory Efficiency Test: 100K Records ===");
        generateTestData(numRecords, testInputPath);
        
        Runtime runtime = Runtime.getRuntime();
        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
        
        long startTime = System.nanoTime();
        pipeline.execute(testInputPath, testOutputPath);
        long endTime = System.nanoTime();
        
        long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
        long memoryUsed = memoryAfter - memoryBefore;
        
        System.out.println("Memory used: " + (memoryUsed / 1024 / 1024) + " MB");
        System.out.println("Execution time: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms");
        
        // Verify pipeline completed without OOM
        assertTrue(Files.exists(Paths.get(testOutputPath, "monthly_sales_summary")));
        
        // Memory assertion: Should use reasonable amount (adjust based on your system)
        assertTrue(memoryUsed < 2L * 1024 * 1024 * 1024, 
            "Pipeline should use less than 2GB for 100K records");
    }
    
    /**
     * Test end-to-end performance with realistic data distribution
     */
    @Test
    @Tag("performance")
    public void testEndToEndPerformance_RealisticData() throws Exception {
        int numRecords = 25_000;
        testInputPath = "test_input_e2e_" + System.currentTimeMillis() + ".csv";
        
        System.out.println("\n=== End-to-End Performance Test: 25K Records ===");
        generateTestData(numRecords, testInputPath);
        
        // Measure full pipeline execution
        long startTime = System.nanoTime();
        pipeline.execute(testInputPath, testOutputPath);
        long endTime = System.nanoTime();
        
        long totalDurationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        double throughput = (double) numRecords / totalDurationMs * 1000;
        
        System.out.println("Total execution time: " + totalDurationMs + " ms");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        
        // Verify all outputs
        Dataset<Row> monthlySummary = spark.read()
            .option("header", "true")
            .csv(testOutputPath + "/monthly_sales_summary");
        Dataset<Row> topProducts = spark.read()
            .option("header", "true")
            .csv(testOutputPath + "/top_products");
        Dataset<Row> anomalies = spark.read()
            .option("header", "true")
            .csv(testOutputPath + "/anomaly_records");
        
        System.out.println("Monthly summary records: " + monthlySummary.count());
        System.out.println("Top products records: " + topProducts.count());
        System.out.println("Anomaly records: " + anomalies.count());
        
        assertTrue(monthlySummary.count() > 0);
        assertTrue(topProducts.count() > 0);
        assertTrue(anomalies.count() > 0 && anomalies.count() <= 5);
        assertTrue(totalDurationMs < 180_000, "End-to-end should complete in under 3 minutes");
    }
    
    /**
     * Helper method to generate test data using SampleDataGenerator
     */
    private void generateTestData(int numRecords, String filePath) throws Exception {
        System.out.println("Generating " + numRecords + " test records...");
        long startTime = System.nanoTime();
        SampleDataGenerator.generateSampleData(numRecords, filePath);
        long endTime = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        System.out.println("Data generation time: " + durationMs + " ms");
        
        // Verify file was created
        Path path = Paths.get(filePath);
        assertTrue(Files.exists(path), "Test data file should be created");
        assertTrue(Files.size(path) > 0, "Test data file should not be empty");
    }
}
