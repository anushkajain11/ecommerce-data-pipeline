package com.example;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Utility class to generate sample e-commerce sales data for testing the pipeline.
 * This generates data with various data quality issues that the pipeline is designed to handle.
 */
public class SampleDataGenerator {
    
    private static final Random random = new Random(42); // Fixed seed for reproducibility
    
    private static final String[] PRODUCTS = {
        "Laptop Computer", "Smart Phone", "Wireless Headphones", "Gaming Mouse",
        "Mechanical Keyboard", "Monitor 27inch", "USB Cable", "Tablet Stand",
        "Webcam HD", "Desk Lamp", "Notebook", "Pen Set", "Stapler", "Paper Shredder",
        "Printer Ink", "Wireless Mouse", "Keyboard Wrist Rest", "Mouse Pad",
        "Monitor Stand", "Desk Organizer", "LED Strip Lights", "Cable Management",
        "Power Strip", "Surge Protector", "Ethernet Cable", "HDMI Cable",
        "Microphone", "Speaker System", "Bluetooth Adapter", "Docking Station"
    };
    
    private static final String[] CATEGORIES = {
        "Electronics", "electronics", "ELECTRONICS", "Electronics ", " electronics",
        "Accessories", "accessories", "Home & Office", "Home & office",
        "Stationery", "stationery", "Stationery ", "Office Supplies",
        "Office Equipment", "office equipment", "Home Decor", "Tools"
    };
    
    private static final String[] REGIONS = {
        "us", "USA", "United States", "united states", "US",
        "uk", "UK", "United Kingdom", "united kingdom",
        "canada", "Canada", "CAN", "Can",
        "Australia", "AUS", "aus", "AUSTRALIA",
        "Germany", "germany", "DE", "de",
        "France", "france", "FR", "fr",
        "India", "india", "IN", "in",
        "China", "china", "CN", "cn",
        "Japan", "japan", "JP", "jp"
    };
    
    private static final String[] DATE_FORMATS = {
        "yyyy-MM-dd",
        "MM/dd/yyyy",
        "dd-MM-yyyy",
        "yyyy/MM/dd",
        "dd/MM/yyyy",
        "MM-dd-yyyy"
    };
    
    public static void main(String[] args) {
        int numRecords = args.length > 0 ? Integer.parseInt(args[0]) : 100000;
        String outputPath = args.length > 1 ? args[1] : "data/sales_sample.csv";
        
        System.out.println("Generating " + numRecords + " sample records...");
        generateSampleData(numRecords, outputPath);
        System.out.println("Sample data generated at: " + outputPath);
    }
    
    /**
     * Generate sample CSV data with various data quality issues
     */
    public static void generateSampleData(int numRecords, String outputPath) {
        try (FileWriter writer = new FileWriter(outputPath)) {
            // Write header
            writer.append("order_id,product_name,category,quantity,unit_price,discount_percent,region,sale_date,revenue\n");
            
            SimpleDateFormat[] dateFormats = new SimpleDateFormat[DATE_FORMATS.length];
            for (int i = 0; i < DATE_FORMATS.length; i++) {
                dateFormats[i] = new SimpleDateFormat(DATE_FORMATS[i]);
            }
            
            Calendar cal = Calendar.getInstance();
            cal.set(2024, 0, 1); // Start from January 1, 2024
            
            for (int i = 1; i <= numRecords; i++) {
                // Generate order_id (with some duplicates)
                String orderId = "ORD" + String.format("%06d", i);
                if (random.nextDouble() < 0.05 && i > 10) {
                    // 5% chance of duplicate order_id
                    orderId = "ORD" + String.format("%06d", random.nextInt(i - 1) + 1);
                }
                
                // Product name (sometimes with inconsistencies)
                String productName = PRODUCTS[random.nextInt(PRODUCTS.length)];
                if (random.nextDouble() < 0.1) {
                    productName = productName.toLowerCase();
                } else if (random.nextDouble() < 0.05) {
                    productName = productName.toUpperCase();
                }
                if (random.nextDouble() < 0.1) {
                    productName = productName.replace(" ", "  "); // Extra spaces
                }
                
                // Category (with typos and variants)
                String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
                
                // Quantity (sometimes as string, sometimes invalid)
                String quantity;
                if (random.nextDouble() < 0.05) {
                    quantity = "abc"; // Invalid
                } else if (random.nextDouble() < 0.1) {
                    quantity = String.valueOf(random.nextInt(10) - 5); // Negative or zero
                } else {
                    quantity = String.valueOf(random.nextInt(100) + 1);
                }
                
                // Unit price (sometimes invalid)
                String unitPrice;
                if (random.nextDouble() < 0.03) {
                    unitPrice = "invalid";
                } else {
                    unitPrice = String.format("%.2f", 5.99 + random.nextDouble() * 995.0);
                }
                
                // Discount percent (sometimes out of range)
                String discountPercent;
                if (random.nextDouble() < 0.05) {
                    discountPercent = String.valueOf(100 + random.nextInt(50)); // > 100
                } else if (random.nextDouble() < 0.03) {
                    discountPercent = String.valueOf(-random.nextInt(20)); // Negative
                } else {
                    discountPercent = String.format("%.1f", random.nextDouble() * 30);
                }
                
                // Region (with typos and variants)
                String region = REGIONS[random.nextInt(REGIONS.length)];
                
                // Sale date (multiple formats, sometimes invalid)
                String saleDate;
                if (random.nextDouble() < 0.03) {
                    saleDate = "invalid-date";
                } else {
                    // Advance calendar by random days (1-365)
                    cal.add(Calendar.DAY_OF_MONTH, random.nextInt(5) + 1);
                    if (cal.get(Calendar.YEAR) > 2024) {
                        cal.set(2024, random.nextInt(12), random.nextInt(28) + 1);
                    }
                    SimpleDateFormat dateFormat = dateFormats[random.nextInt(dateFormats.length)];
                    saleDate = dateFormat.format(cal.getTime());
                }
                
                // Revenue (will be recalculated by pipeline)
                String revenue = String.format("%.2f", random.nextDouble() * 2000);
                
                // Write record
                writer.append(String.join(",",
                    orderId,
                    "\"" + productName + "\"",
                    category,
                    quantity,
                    unitPrice,
                    discountPercent,
                    region,
                    saleDate,
                    revenue
                ));
                writer.append("\n");
                
                if (i % 1000 == 0) {
                    System.out.println("Generated " + i + " records...");
                }
            }
            
        } catch (IOException e) {
            System.err.println("Error generating sample data: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

