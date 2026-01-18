package com.example;

import org.apache.spark.sql.SparkSession;

/**
 * Main entry point for the E-commerce Data Pipeline
 * 
 * Usage:
 *   java -cp target/classes com.example.Main <input_csv_path> <output_path>
 * 
 * Example:
 *   java -cp target/classes com.example.Main data/sales.csv output/
 */
public class Main {
    public static void main(String[] args) {
        // Default paths if not provided
        String inputPath = args.length > 0 ? args[0] : "data/sales_large.csv";
        String outputPath = args.length > 1 ? args[1] : "output/";
        
        System.out.println("Initializing Spark Session...");
        SparkSession spark = SparkSession.builder()
            .appName("EcommercePipeline")
            .master("local[*]") // Use local[*] for local testing, or "yarn" for cluster
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            // Note: KryoSerializer removed for Java 17+ compatibility
            // For production, uncomment and add JVM args: --add-opens=java.base/java.nio=ALL-UNNAMED
            // .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate();
        
        try {
            // Create and execute pipeline
            EcommercePipeline pipeline = new EcommercePipeline(spark);
            pipeline.execute(inputPath, outputPath);
            
            System.out.println("Pipeline completed successfully!");
        } catch (Exception e) {
            System.err.println("Error executing pipeline: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}