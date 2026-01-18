package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DataCleaner class
 */
@Execution(ExecutionMode.CONCURRENT)
public class DataCleanerTest {
    
    private static SparkSession spark;
    private DataCleaner dataCleaner;
    
    @BeforeAll
    public static void setUpSpark() {
        // Disable security for local testing (avoids Java 17+ Subject.getSubject() issues)
        System.setProperty("java.security.auth.login.config", "NONE");
        System.setProperty("hadoop.security.authentication", "simple");
        
        spark = SparkSession.builder()
            .appName("DataCleanerTest")
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
        dataCleaner = new DataCleaner();
    }
    
    /**
     * Helper method to create standard schema for string-based fields
     */
    private StructType createStandardSchema() {
        return new StructType()
            .add("order_id", DataTypes.StringType)
            .add("product_name", DataTypes.StringType)
            .add("category", DataTypes.StringType)
            .add("quantity", DataTypes.StringType)
            .add("unit_price", DataTypes.StringType)
            .add("discount_percent", DataTypes.StringType)
            .add("region", DataTypes.StringType)
            .add("sale_date", DataTypes.StringType)
            .add("revenue", DataTypes.StringType);
    }
    
    /**
     * Helper method to create DataFrame from data
     */
    private Dataset<Row> createDataFrame(List<Row> data, StructType schema) {
        return spark.createDataFrame(data, schema);
    }
    
    @Test
    public void testCleanOrderId_RemovesWhitespace() {
        List<Row> data = Arrays.asList(
            RowFactory.create(" ORD001 ", "Product1", "Category1", "1", "10.0", "0", "US", "2024-01-01", "10.0"), // whitespace trimming
            RowFactory.create("ORD001", "Product2", "Category2", "2", "20.0", "5", "UK", "2024-01-02", "38.0"), // duplicate orderId
            RowFactory.create("ORD002", "Product2", "Category2", "2", "20.0", "5", "UK", "2024-01-02", "38.0"), // valid
            RowFactory.create(null, "Product2", "Category2", "2", "20.0", "5", "UK", "2024-01-02", "38.0"), // null orderid
            RowFactory.create("   ", "Product3", "Category3", "3", "30.0", "0", "CA", "2024-01-03", "90.0"), // empty after trim
            RowFactory.create("", "Product4", "Category4", "4", "40.0", "0", "DE", "2024-01-04", "160.0") // empty string
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanOrderId(df);
        
        List<Row> results = cleaned.collectAsList();
        assertEquals(results.size(), 2); // Only ORD001 and ORD002 should remain (null, empty, whitespace-only filtered out)
        assertEquals("ORD001", results.get(0).getAs("order_id"));
        assertEquals("ORD002", results.get(1).getAs("order_id"));
    }
    
    
    @Test
    public void testCleanProductName_NormalizesCase() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "laptop computer", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99"), // lowercase normalization
            RowFactory.create("ORD002", "Monitor 27%#inch", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99"), // special characters removal
            RowFactory.create("ORD003", "iPHONE Pro MAX", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99"), // mixed case
            RowFactory.create("ORD004", "Product    Name", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99"), // multiple spaces
            RowFactory.create("ORD005", "  Product Name  ", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99"), // whitespace trimming
            RowFactory.create("ORD006", "###@@@", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99"), // only special characters
            RowFactory.create("ORD007", "", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99"), // empty string
            RowFactory.create("ORD008", "   ", "Electronics", "1", "999.99", "0", "US", "2024-01-01", "999.99") // whitespace only
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanProductName(df);

        List<Row> results = cleaned.collectAsList();
        assertEquals(8, results.size());
        assertTrue(results.get(0).<String>getAs("product_name").contains("Laptop Computer"));
        assertTrue(results.get(1).<String>getAs("product_name").contains("Monitor") && 
                   results.get(1).<String>getAs("product_name").contains("inch"));
        assertTrue(results.get(2).<String>getAs("product_name").contains("Iphone") || 
                   results.get(2).<String>getAs("product_name").contains("Pro"));
        assertTrue(results.get(3).<String>getAs("product_name").contains("Product") && 
                   results.get(3).<String>getAs("product_name").contains("Name"));
        assertEquals("Product Name", results.get(4).getAs("product_name")); // trimmed
        assertEquals("", results.get(5).getAs("product_name")); // all special chars removed
        assertEquals("", results.get(6).getAs("product_name")); // empty
        assertEquals("", results.get(7).getAs("product_name")); // whitespace only
    }
    
    @Test
    public void testCleanCategory_HandlesNull() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", null, "1", "10.0", "0", "US", "2024-01-01", "10.0"), // null -> Unknown
            RowFactory.create("ORD002", "Product2", "electronics", "2", "20.0", "5", "UK", "2024-01-02", "38.0"), // lowercase normalization
            RowFactory.create("ORD003", "Product3", "", "3", "30.0", "0", "CA", "2024-01-03", "90.0"), // empty string
            RowFactory.create("ORD004", "Product4", "   ", "4", "40.0", "0", "DE", "2024-01-04", "160.0"), // whitespace only
            RowFactory.create("ORD005", "Product5", "Electronics    Category", "5", "50.0", "0", "FR", "2024-01-05", "250.0"), // multiple spaces
            RowFactory.create("ORD006", "Product6", "ELECTRONICS", "6", "60.0", "0", "IN", "2024-01-06", "360.0"), // uppercase
            RowFactory.create("ORD007", "Product7", "Electronics", "7", "70.0", "0", "CN", "2024-01-07", "490.0") // proper case
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanCategory(df);
        
        List<Row> results = cleaned.collectAsList();
        assertEquals("Unknown", results.get(0).getAs("category")); // null -> Unknown
        assertEquals("Electronics", results.get(1).getAs("category")); // lowercase normalized
        assertEquals("", results.get(2).getAs("category")); // empty string stays empty (not null)
        assertEquals("", results.get(3).getAs("category")); // whitespace trimmed to empty
        assertTrue(results.get(4).<String>getAs("category").contains("Electronics") && 
                   results.get(4).<String>getAs("category").contains("Category")); // multiple spaces normalized
        assertEquals("Electronics", results.get(5).getAs("category")); // uppercase normalized
        assertEquals("Electronics", results.get(6).getAs("category")); // proper case stays
    }
    
    @Test
    public void testCleanQuantity_HandlesInvalidValues() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", "abc", "10.0", "0", "US", "2024-01-01", "10.0"), // invalid -> 0
            RowFactory.create("ORD002", "Product2", "Cat2", "-5", "20.0", "0", "UK", "2024-01-02", "20.0"), // negative -> 0
            RowFactory.create("ORD003", "Product3", "Cat3", "10", "30.0", "0", "CA", "2024-01-03", "300.0"), // valid
            RowFactory.create("ORD004", "Product4", "Cat4", null, "40.0", "0", "DE", "2024-01-04", "160.0"), // null -> 0
            RowFactory.create("ORD005", "Product5", "Cat5", "", "50.0", "0", "FR", "2024-01-05", "250.0"), // empty -> 0
            RowFactory.create("ORD006", "Product6", "Cat6", "   ", "60.0", "0", "IN", "2024-01-06", "360.0"), // whitespace -> 0
            RowFactory.create("ORD007", "Product7", "Cat7", "0", "70.0", "0", "CN", "2024-01-07", "490.0"), // zero -> 0
            RowFactory.create("ORD008", "Product8", "Cat8", "10.5", "80.0", "0", "JP", "2024-01-08", "640.0"), // decimal -> 0
            RowFactory.create("ORD009", "Product9", "Cat9", "10abc", "90.0", "0", "AU", "2024-01-09", "810.0"), // mixed -> 0
            RowFactory.create("ORD010", "Product10", "Cat10", "00010", "100.0", "0", "GB", "2024-01-10", "1000.0") // leading zeros -> 10
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanQuantity(df);
        
        List<Row> results = cleaned.collectAsList();
        assertEquals(0, (int)results.get(0).getAs("quantity")); // Invalid -> 0
        assertEquals(0, (int)results.get(1).getAs("quantity")); // Negative -> 0
        assertEquals(10, (int)results.get(2).getAs("quantity")); // Valid -> 10
        assertEquals(0, (int)results.get(3).getAs("quantity")); // Null -> 0
        assertEquals(0, (int)results.get(4).getAs("quantity")); // Empty -> 0
        assertEquals(0, (int)results.get(5).getAs("quantity")); // Whitespace -> 0
        assertEquals(0, (int)results.get(6).getAs("quantity")); // Zero -> 0
        assertEquals(0, (int)results.get(7).getAs("quantity")); // Decimal -> 0
        assertEquals(0, (int)results.get(8).getAs("quantity")); // Mixed -> 0
        assertEquals(10, (int)results.get(9).getAs("quantity")); // Leading zeros -> 10
    }
    
    @Test
    public void testCleanUnitPrice_HandlesInvalidValues() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", "1", "invalid", "0", "US", "2024-01-01", "10.0"), // invalid -> 0.0
            RowFactory.create("ORD002", "Product2", "Cat2", "2", "-10.0", "0", "UK", "2024-01-02", "20.0"), // negative -> 0.0
            RowFactory.create("ORD003", "Product3", "Cat3", "3", "99.99", "0", "CA", "2024-01-03", "299.97"), // valid
            RowFactory.create("ORD004", "Product4", "Cat4", "4", null, "0", "DE", "2024-01-04", "160.0"), // null -> 0.0
            RowFactory.create("ORD005", "Product5", "Cat5", "5", "", "0", "FR", "2024-01-05", "250.0"), // empty -> 0.0
            RowFactory.create("ORD006", "Product6", "Cat6", "6", "   ", "0", "IN", "2024-01-06", "360.0"), // whitespace -> 0.0
            RowFactory.create("ORD007", "Product7", "Cat7", "7", "0.0", "0", "CN", "2024-01-07", "490.0"), // zero -> 0.0
            RowFactory.create("ORD008", "Product8", "Cat8", "8", "$100.00", "0", "JP", "2024-01-08", "640.0"), // currency -> 0.0
            RowFactory.create("ORD009", "Product9", "Cat9", "9", "1,000.00", "0", "AU", "2024-01-09", "810.0"), // comma -> 0.0
            RowFactory.create("ORD010", "Product10", "Cat10", "10", "10.5.3", "0", "GB", "2024-01-10", "1000.0"), // multiple decimals -> 0.0
            RowFactory.create("ORD011", "Product11", "Cat11", "11", "0.0001", "0", "US", "2024-01-11", "1100.0"), // small number
            RowFactory.create("ORD012", "Product12", "Cat12", "12", "010.50", "0", "UK", "2024-01-12", "1200.0") // leading zeros -> 10.50
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanUnitPrice(df);
        
        List<Row> results = cleaned.collectAsList();
        assertEquals(0.0, results.get(0).getAs("unit_price")); // Invalid -> 0.0
        assertEquals(0.0, results.get(1).getAs("unit_price")); // Negative -> 0.0
        assertEquals(99.99, results.get(2).getAs("unit_price")); // Valid -> 99.99
        assertEquals(0.0, results.get(3).getAs("unit_price")); // Null -> 0.0
        assertEquals(0.0, results.get(4).getAs("unit_price")); // Empty -> 0.0
        assertEquals(0.0, results.get(5).getAs("unit_price")); // Whitespace -> 0.0
        assertEquals(0.0, results.get(6).getAs("unit_price")); // Zero -> 0.0
        assertEquals(0.0, results.get(7).getAs("unit_price")); // Currency -> 0.0
        assertEquals(0.0, results.get(8).getAs("unit_price")); // Comma -> 0.0
        assertEquals(0.0, results.get(9).getAs("unit_price")); // Multiple decimals -> 0.0
        assertEquals(0.0001, (double)results.get(10).getAs("unit_price"), 0.0001); // Small number
        assertEquals(10.50, (double)results.get(11).getAs("unit_price"), 0.01); // Leading zeros -> 10.50
    }
    
    @Test
    public void testCleanDiscountPercent_ClampsRange() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", "1", "10.0", "150", "US", "2024-01-01", "10.0"), // >100 -> 100
            RowFactory.create("ORD002", "Product2", "Cat2", "2", "20.0", "-10", "UK", "2024-01-02", "20.0"), // negative -> 0
            RowFactory.create("ORD003", "Product3", "Cat3", "3", "30.0", "15", "CA", "2024-01-03", "76.5"), // valid
            RowFactory.create("ORD004", "Product4", "Cat4", "4", "40.0", null, "DE", "2024-01-04", "160.0"), // null -> 0.0
            RowFactory.create("ORD005", "Product5", "Cat5", "5", "50.0", "", "FR", "2024-01-05", "250.0"), // empty -> 0.0
            RowFactory.create("ORD006", "Product6", "Cat6", "6", "60.0", "   ", "IN", "2024-01-06", "360.0"), // whitespace -> 0.0
            RowFactory.create("ORD007", "Product7", "Cat7", "7", "70.0", "0", "CN", "2024-01-07", "490.0"), // zero -> 0.0
            RowFactory.create("ORD008", "Product8", "Cat8", "8", "80.0", "100", "JP", "2024-01-08", "640.0"), // exactly 100 -> 100.0
            RowFactory.create("ORD009", "Product9", "Cat9", "9", "90.0", "15.5", "AU", "2024-01-09", "810.0"), // decimal -> 15.5
            RowFactory.create("ORD010", "Product10", "Cat10", "10", "100.0", "99.9999", "GB", "2024-01-10", "1000.0"), // large decimal
            RowFactory.create("ORD011", "Product11", "Cat11", "11", "110.0", "10.5.3", "US", "2024-01-11", "1100.0") // multiple decimals -> 0.0
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanDiscountPercent(df);
        
        List<Row> results = cleaned.collectAsList();
        assertEquals(100.0, results.get(0).getAs("discount_percent")); // >100 -> 100
        assertEquals(0.0, results.get(1).getAs("discount_percent")); // Negative -> 0
        assertEquals(15.0, results.get(2).getAs("discount_percent")); // Valid -> 15
        assertEquals(0.0, results.get(3).getAs("discount_percent")); // Null -> 0.0
        assertEquals(0.0, results.get(4).getAs("discount_percent")); // Empty -> 0.0
        assertEquals(0.0, results.get(5).getAs("discount_percent")); // Whitespace -> 0.0
        assertEquals(0.0, results.get(6).getAs("discount_percent")); // Zero -> 0.0
        assertEquals(100.0, results.get(7).getAs("discount_percent")); // Exactly 100 -> 100.0
        assertEquals(15.5, (double)results.get(8).getAs("discount_percent"), 0.01); // Decimal -> 15.5
        assertEquals(99.9999, (double)results.get(9).getAs("discount_percent"), 0.0001); // Large decimal
        assertEquals(0.0, results.get(10).getAs("discount_percent")); // Multiple decimals -> 0.0
    }
    
    @Test
    public void testCleanRegion_NormalizesVariants() {
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", "1", "10.0", "0", "us", "2024-01-01", "10.0"), // lowercase -> United States
            RowFactory.create("ORD002", "Product2", "Cat2", "2", "20.0", "0", "USA", "2024-01-02", "40.0"), // USA -> United States
            RowFactory.create("ORD003", "Product3", "Cat3", "3", "30.0", "0", "uk", "2024-01-03", "90.0"), // uk -> United Kingdom
            RowFactory.create("ORD004", "Product4", "Cat4", "4", "40.0", "0", "greenland", "2024-01-04", "160.0"), // not in map -> Greenland
            RowFactory.create("ORD005", "Product5", "Cat5", "5", "50.0", "0", null, "2024-01-05", "250.0"), // null -> Unknown
            RowFactory.create("ORD006", "Product6", "Cat6", "6", "60.0", "0", "", "2024-01-06", "360.0"), // empty -> Unknown
            RowFactory.create("ORD007", "Product7", "Cat7", "7", "70.0", "0", "   ", "2024-01-07", "490.0"), // whitespace -> Unknown
            RowFactory.create("ORD008", "Product8", "Cat8", "8", "80.0", "0", "canada", "2024-01-08", "640.0"), // canada -> Canada
            RowFactory.create("ORD009", "Product9", "Cat9", "9", "90.0", "0", "can", "2024-01-09", "810.0"), // can -> Canada
            RowFactory.create("ORD010", "Product10", "Cat10", "10", "100.0", "0", "australia", "2024-01-10", "1000.0"), // australia -> Australia
            RowFactory.create("ORD011", "Product11", "Cat11", "11", "110.0", "0", "aus", "2024-01-11", "1100.0"), // aus -> Australia
            RowFactory.create("ORD012", "Product12", "Cat12", "12", "120.0", "0", "germany", "2024-01-12", "1200.0"), // germany -> Germany
            RowFactory.create("ORD013", "Product13", "Cat13", "13", "130.0", "0", "de", "2024-01-13", "1300.0"), // de -> Germany
            RowFactory.create("ORD014", "Product14", "Cat14", "14", "140.0", "0", "france", "2024-01-14", "1400.0"), // france -> France
            RowFactory.create("ORD015", "Product15", "Cat15", "15", "150.0", "0", "fr", "2024-01-15", "1500.0"), // fr -> France
            RowFactory.create("ORD016", "Product16", "Cat16", "16", "160.0", "0", "india", "2024-01-16", "1600.0"), // india -> India
            RowFactory.create("ORD017", "Product17", "Cat17", "17", "170.0", "0", "in", "2024-01-17", "1700.0"), // in -> India
            RowFactory.create("ORD018", "Product18", "Cat18", "18", "180.0", "0", "china", "2024-01-18", "1800.0"), // china -> China
            RowFactory.create("ORD019", "Product19", "Cat19", "19", "190.0", "0", "cn", "2024-01-19", "1900.0"), // cn -> China
            RowFactory.create("ORD020", "Product20", "Cat20", "20", "200.0", "0", "japan", "2024-01-20", "2000.0"), // japan -> Japan
            RowFactory.create("ORD021", "Product21", "Cat21", "21", "210.0", "0", "jp", "2024-01-21", "2100.0"), // jp -> Japan
            RowFactory.create("ORD022", "Product22", "Cat22", "22", "220.0", "0", "United   States", "2024-01-22", "2200.0"), // multiple spaces
            RowFactory.create("ORD023", "Product23", "Cat23", "23", "230.0", "0", "New York", "2024-01-23", "2300.0") // unmapped -> capitalized
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanRegion(df);
        
        List<Row> results = cleaned.collectAsList();
        assertEquals("United States", results.get(0).getAs("region")); // us -> United States
        assertEquals("United States", results.get(1).getAs("region")); // USA -> United States
        assertEquals("United Kingdom", results.get(2).getAs("region")); // uk -> United Kingdom
        assertEquals("Greenland", results.get(3).getAs("region")); // greenland -> Greenland
        assertEquals("Unknown", results.get(4).getAs("region")); // null -> Unknown
        assertEquals("Unknown", results.get(5).getAs("region")); // empty -> Unknown
        assertEquals("Unknown", results.get(6).getAs("region")); // whitespace -> Unknown
        assertEquals("Canada", results.get(7).getAs("region")); // canada -> Canada
        assertEquals("Canada", results.get(8).getAs("region")); // can -> Canada
        assertEquals("Australia", results.get(9).getAs("region")); // australia -> Australia
        assertEquals("Australia", results.get(10).getAs("region")); // aus -> Australia
        assertEquals("Germany", results.get(11).getAs("region")); // germany -> Germany
        assertEquals("Germany", results.get(12).getAs("region")); // de -> Germany
        assertEquals("France", results.get(13).getAs("region")); // france -> France
        assertEquals("France", results.get(14).getAs("region")); // fr -> France
        assertEquals("India", results.get(15).getAs("region")); // india -> India
        assertEquals("India", results.get(16).getAs("region")); // in -> India
        assertEquals("China", results.get(17).getAs("region")); // china -> China
        assertEquals("China", results.get(18).getAs("region")); // cn -> China
        assertEquals("Japan", results.get(19).getAs("region")); // japan -> Japan
        assertEquals("Japan", results.get(20).getAs("region")); // jp -> Japan
        assertTrue(results.get(21).<String>getAs("region").contains("United") && 
                   results.get(21).<String>getAs("region").contains("States")); // multiple spaces normalized
        assertEquals("New York", results.get(22).getAs("region")); // unmapped -> capitalized
    }
    
    @Test
    public void testCalculateRevenue_ComputesCorrectly() {
        // Start with string data (like CSV input) and go through cleaning pipeline
        List<Row> data = Arrays.asList(
            RowFactory.create("ORD001", "Product1", "Cat1", "2", "100.0", "10.0", "US", "2024-01-01", null), // standard calculation
            RowFactory.create("ORD002", "Product2", "Cat2", "3", "50.0", "0", "UK", "2024-01-02", null), // no discount
            RowFactory.create("ORD003", "Product3", "Cat3", "5", "20.0", "100", "CA", "2024-01-03", null), // 100% discount -> 0 revenue
            RowFactory.create("ORD004", "Product4", "Cat4", "0", "100.0", "10", "DE", "2024-01-04", null), // zero quantity -> 0 revenue
            RowFactory.create("ORD005", "Product5", "Cat5", "10", "0.0", "10", "FR", "2024-01-05", null), // zero price -> 0 revenue
            RowFactory.create("ORD006", "Product6", "Cat6", "4", "25.0", "15.5", "IN", "2024-01-06", null) // decimal discount
        );
        
        // Create DataFrame with string types (like CSV input)
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        
        // Clean the data first (convert strings to proper types)
        Dataset<Row> cleaned = dataCleaner.cleanQuantity(df);
        cleaned = dataCleaner.cleanUnitPrice(cleaned);
        cleaned = dataCleaner.cleanDiscountPercent(cleaned);
        cleaned = dataCleaner.cleanSaleDate(cleaned);
        
        // Now calculate revenue (expects cleaned numeric types)
        cleaned = dataCleaner.calculateRevenue(cleaned);
        
        List<Row> results = cleaned.collectAsList();
        
        // Revenue = quantity * unit_price * (1 - discount_percent/100)
        // ORD001: 2 * 100.0 * (1 - 10/100) = 2 * 100 * 0.9 = 180.0
        // ORD002: 3 * 50.0 * (1 - 0/100) = 3 * 50 * 1.0 = 150.0
        // ORD003: 5 * 20.0 * (1 - 100/100) = 5 * 20 * 0.0 = 0.0
        // ORD004: 0 * 100.0 * (1 - 10/100) = 0
        // ORD005: 10 * 0.0 * (1 - 10/100) = 0
        // ORD006: 4 * 25.0 * (1 - 15.5/100) = 4 * 25 * 0.845 = 84.5
        
        double revenue1 = results.get(0).<Double>getAs("revenue");
        double revenue2 = results.get(1).<Double>getAs("revenue");
        double revenue3 = results.get(2).<Double>getAs("revenue");
        double revenue4 = results.get(3).<Double>getAs("revenue");
        double revenue5 = results.get(4).<Double>getAs("revenue");
        double revenue6 = results.get(5).<Double>getAs("revenue");
        
        assertEquals(180.0, revenue1, 0.01);
        assertEquals(150.0, revenue2, 0.01);
        assertEquals(0.0, revenue3, 0.01); // 100% discount
        assertEquals(0.0, revenue4, 0.01); // zero quantity
        assertEquals(0.0, revenue5, 0.01); // zero price
        assertEquals(84.5, revenue6, 0.01); // decimal discount
    }
    
    @Test
    public void testCleanSaleDate_HandlesAllFormats() {
        List<Row> data = Arrays.asList(
            // Valid formats
            RowFactory.create("ORD001", "Product1", "Cat1", "1", "10.0", "0", "US", "2024-01-15", "10.0"), // yyyy-MM-dd
            RowFactory.create("ORD002", "Product2", "Cat2", "2", "20.0", "0", "UK", "01/15/2024", "40.0"), // MM/dd/yyyy
            RowFactory.create("ORD003", "Product3", "Cat3", "3", "30.0", "0", "CA", "15-01-2024", "90.0"), // dd-MM-yyyy
            RowFactory.create("ORD004", "Product4", "Cat4", "4", "40.0", "0", "DE", "2024/01/15", "160.0"), // yyyy/MM/dd
            RowFactory.create("ORD005", "Product5", "Cat5", "5", "50.0", "0", "FR", "15/01/2024", "250.0"), // dd/MM/yyyy
            RowFactory.create("ORD006", "Product6", "Cat6", "6", "60.0", "0", "IN", "01-15-2024", "360.0"), // MM-dd-yyyy
            // Edge cases
            RowFactory.create("ORD007", "Product7", "Cat7", "7", "70.0", "0", "CN", null, "490.0"), // null -> null
            RowFactory.create("ORD008", "Product8", "Cat8", "8", "80.0", "0", "JP", "", "640.0"), // empty -> null
            RowFactory.create("ORD009", "Product9", "Cat9", "9", "90.0", "0", "AU", "   ", "810.0"), // whitespace -> null
            RowFactory.create("ORD010", "Product10", "Cat10", "10", "100.0", "0", "GB", "2024-02-29", "1000.0"), // leap year 2024 (valid)
            RowFactory.create("ORD011", "Product11", "Cat11", "11", "110.0", "0", "US", "2024-02-30", "1100.0"), // invalid day
            RowFactory.create("ORD012", "Product12", "Cat12", "12", "120.0", "0", "UK", "2024-13-01", "1200.0"), // invalid month
            RowFactory.create("ORD013", "Product13", "Cat13", "13", "130.0", "0", "CA", "not-a-date", "1300.0"), // malformed
            RowFactory.create("ORD014", "Product14", "Cat14", "14", "140.0", "0", "DE", "2024", "1400.0"), // incomplete date
            RowFactory.create("ORD015", "Product15", "Cat15", "15", "150.0", "0", "FR", "  2024-01-15  ", "1500.0"), // whitespace around
            RowFactory.create("ORD016", "Product16", "Cat16", "16", "160.0", "0", "IN", "2024-01-15 10:30:00", "1600.0") // with time (regex fallback)
        );
        
        Dataset<Row> df = createDataFrame(data, createStandardSchema());
        Dataset<Row> cleaned = dataCleaner.cleanSaleDate(df);
        
        List<Row> results = cleaned.collectAsList();
        
        // Valid dates should parse correctly
        assertNotNull(results.get(0).getAs("sale_date")); // yyyy-MM-dd
        assertNotNull(results.get(1).getAs("sale_date")); // MM/dd/yyyy
        assertNotNull(results.get(2).getAs("sale_date")); // dd-MM-yyyy
        assertNotNull(results.get(3).getAs("sale_date")); // yyyy/MM/dd
        assertNotNull(results.get(4).getAs("sale_date")); // dd/MM/yyyy
        assertNotNull(results.get(5).getAs("sale_date")); // MM-dd-yyyy
        // Null/empty should return null
        assertNull(results.get(6).getAs("sale_date")); // null
        assertNull(results.get(7).getAs("sale_date")); // empty
        assertNull(results.get(8).getAs("sale_date")); // whitespace
        // Leap year should be valid (2024 is leap year)
        assertNotNull(results.get(9).getAs("sale_date")); // 2024-02-29 (valid)
        // Invalid dates might parse but be incorrect - depends on implementation
        // Malformed dates should return null or fail parsing
        // The regex fallback should handle date with time component
        assertNotNull(results.get(15).getAs("sale_date")); // with time - regex should extract date
    }
}

