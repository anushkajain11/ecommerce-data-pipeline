package com.example;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * Data cleaning utilities for handling data quality issues
 */
public class DataCleaner {
    
    // Common date formats to try
    private static final String[] DATE_FORMATS = {
        "yyyy-MM-dd",
        "MM/dd/yyyy",
        "dd-MM-yyyy",
        "yyyy/MM/dd",
        "dd/MM/yyyy",
        "MM-dd-yyyy"
    };
    
    // Region normalization map (common typos and variants)
    private static final java.util.Map<String, String> REGION_MAP = new java.util.HashMap<>();
    static {
        REGION_MAP.put("us", "United States");
        REGION_MAP.put("usa", "United States");
        REGION_MAP.put("united states", "United States");
        REGION_MAP.put("uk", "United Kingdom");
        REGION_MAP.put("united kingdom", "United Kingdom");
        REGION_MAP.put("canada", "Canada");
        REGION_MAP.put("can", "Canada");
        REGION_MAP.put("australia", "Australia");
        REGION_MAP.put("aus", "Australia");
        REGION_MAP.put("germany", "Germany");
        REGION_MAP.put("de", "Germany");
        REGION_MAP.put("france", "France");
        REGION_MAP.put("fr", "France");
        REGION_MAP.put("india", "India");
        REGION_MAP.put("in", "India");
        REGION_MAP.put("china", "China");
        REGION_MAP.put("cn", "China");
        REGION_MAP.put("japan", "Japan");
        REGION_MAP.put("jp", "Japan");
    }
    
    /**
     * Clean order_id: remove duplicates, trim whitespace, handle nulls
     */
    public Dataset<Row> cleanOrderId(Dataset<Row> df) {
        return df
            .withColumn("order_id", trim(col("order_id")))
            .filter(col("order_id").isNotNull()) 
            .filter(length(col("order_id")).gt(0))
            .dropDuplicates("order_id");
    }
    
    /**
     * Clean product_name: trim, normalize case, remove special characters
     */
    public Dataset<Row> cleanProductName(Dataset<Row> df) {
        return df.withColumn("product_name",
            trim(
                regexp_replace(
                    initcap(col("product_name")),
                    "[^a-zA-Z0-9\\s-]", ""
                )
            )
        );
    }
    
    /**
     * Clean category: normalize case, fix common typos
     */
    public Dataset<Row> cleanCategory(Dataset<Row> df) {
        return df.withColumn("category",
            when(col("category").isNull(), lit("Unknown"))
            .otherwise(
                trim(
                    initcap(
                        regexp_replace(
                            lower(col("category")),
                            "\\s+", " "
                        )
                    )
                )
            )
        );
    }
    
    /**
     * Clean quantity: convert string to integer, handle invalid values
     */
    public Dataset<Row> cleanQuantity(Dataset<Row> df) {
        return df.withColumn("quantity",
            when(
                col("quantity").isNull()
                .or(trim(col("quantity")).equalTo(""))
                .or(not(trim(col("quantity")).rlike("^-?\\d+$"))),
                lit(0)
            )
            .otherwise(
                when(
                    trim(col("quantity")).cast(DataTypes.IntegerType).leq(0),
                    lit(0)
                )
                .otherwise(
                    trim(col("quantity")).cast(DataTypes.IntegerType)
                )
            )
        );
    }
    
    /**
     * Clean unit_price: convert string to double, validate range
     */
    public Dataset<Row> cleanUnitPrice(Dataset<Row> df) {
        return df.withColumn("unit_price",
            when(
                col("unit_price").isNull()
                .or(trim(col("unit_price")).equalTo(""))
                .or(not(trim(col("unit_price")).rlike("^-?\\d+(\\.\\d+)?$"))),
                lit(0.0)
            )
            .otherwise(
                when(
                    trim(col("unit_price")).cast(DataTypes.DoubleType).leq(0),
                    lit(0.0)
                )
                .otherwise(
                    trim(col("unit_price")).cast(DataTypes.DoubleType)
                )
            )
        );
    }
    
    /**
     * Clean discount_percent: convert to double, validate range (0-100)
     */
    public Dataset<Row> cleanDiscountPercent(Dataset<Row> df) {
        return df.withColumn("discount_percent",
            when(
                col("discount_percent").isNull()
                .or(trim(col("discount_percent")).equalTo(""))
                .or(not(trim(col("discount_percent")).rlike("^-?\\d+(\\.\\d+)?$"))),
                lit(0.0)
            )
            .otherwise(
                when(
                    trim(col("discount_percent")).cast(DataTypes.DoubleType).lt(0),
                    lit(0.0)
                )
                .when(
                    trim(col("discount_percent")).cast(DataTypes.DoubleType).gt(100),
                    lit(100.0)
                )
                .otherwise(
                    trim(col("discount_percent")).cast(DataTypes.DoubleType)
                )
            )
        );
    }
    
    /**
     * Clean region: normalize variants and fix typos
     */
    public Dataset<Row> cleanRegion(Dataset<Row> df) {
        // Create a UDF for region normalization
        org.apache.spark.sql.expressions.UserDefinedFunction normalizeRegion = udf(
            (String region) -> {
                if (region == null || region.trim().isEmpty()) {
                    return "Unknown";
                }
                String normalized = region.trim().toLowerCase();
                String mapped = REGION_MAP.get(normalized);
                if (mapped != null) {
                    return mapped;
                }
                // Capitalize first letter of each word
                String[] words = region.trim().split("\\s+");
                StringBuilder result = new StringBuilder();
                for (int i = 0; i < words.length; i++) {
                    if (words[i].length() > 0) {
                        result.append(Character.toUpperCase(words[i].charAt(0)));
                        if (words[i].length() > 1) {
                            result.append(words[i].substring(1).toLowerCase());
                        }
                        if (i < words.length - 1) {
                            result.append(" ");
                        }
                    }
                }
                return result.toString();
            },
            DataTypes.StringType
        );
        
        return df.withColumn("region", normalizeRegion.apply(col("region")));
    }
    
    /**
     * Clean sale_date: parse multiple date formats, handle nulls
     */
    public Dataset<Row> cleanSaleDate(Dataset<Row> df) {
        // Create UDF to parse dates with multiple formats
        org.apache.spark.sql.expressions.UserDefinedFunction parseDate = udf(
            (String dateStr) -> {
                if (dateStr == null || dateStr.trim().isEmpty()) {
                    return null;
                }
                
                String trimmed = dateStr.trim();
                
                // Try each date format
                for (String format : DATE_FORMATS) {
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat(format);
                        sdf.setLenient(false);
                        java.util.Date date = sdf.parse(trimmed);
                        // Return in standard format
                        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
                        return outputFormat.format(date);
                    } catch (Exception e) {
                        // Try next format
                    }
                }
                
                // If all formats fail, try to extract date-like patterns
                Pattern datePattern = Pattern.compile("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})");
                java.util.regex.Matcher matcher = datePattern.matcher(trimmed);
                if (matcher.find()) {
                    String year = matcher.group(1);
                    String month = String.format("%02d", Integer.parseInt(matcher.group(2)));
                    String day = String.format("%02d", Integer.parseInt(matcher.group(3)));
                    return year + "-" + month + "-" + day;
                }
                
                return null;
            },
            DataTypes.StringType
        );
        
        return df.withColumn("sale_date", 
            to_date(parseDate.apply(col("sale_date")), "yyyy-MM-dd")
        );
    }
    
    /**
     * Calculate revenue: quantity * unit_price * (1 - discount_percent/100)
     */
    public Dataset<Row> calculateRevenue(Dataset<Row> df) {
        return df.withColumn("revenue",
            col("quantity")
                .multiply(col("unit_price"))
                .multiply(
                    lit(1.0).minus(col("discount_percent").divide(lit(100.0)))
                )
        );
    }
}

