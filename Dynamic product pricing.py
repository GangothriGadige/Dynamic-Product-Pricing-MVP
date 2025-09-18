# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

# -----------------------------
# 1. Product metadata
# -----------------------------
product_data = [
    ("SKU-1001", "Consumables", "Box of 100", "Medico", "Direct"),
    ("SKU-2001", "Instruments", "Unit", "LabTech", "Supplier"),
    ("SKU-3001", "Reagents", "500ml Bottle", "ChemLabs", "Direct")
]
product_schema = T.StructType([
    T.StructField("sku_id", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("packaging", T.StringType(), True),
    T.StructField("manufacturer", T.StringType(), True),
    T.StructField("fulfillment_method", T.StringType(), True)
])
df_products = spark.createDataFrame(product_data, product_schema)

# -----------------------------
# 2. Transactional data
# -----------------------------
txn_data = [
    ("SKU-1001", 12.0, 2, "2025-09-01", "New York"),
    ("SKU-1001", 11.5, 1, "2025-09-05", "Boston"),
    ("SKU-2001", 320.0, 1, "2025-09-02", "Chicago"),
    ("SKU-3001", 60.0, 3, "2025-09-03", "San Francisco")
]
txn_schema = T.StructType([
    T.StructField("sku_id", T.StringType(), True),
    T.StructField("price_paid", T.DoubleType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("customer_location", T.StringType(), True)
])
df_txn = spark.createDataFrame(txn_data, txn_schema)

# -----------------------------
# 3. Supplier data
# -----------------------------
supplier_data = [
    ("SKU-1001", 9.0, "Available", 3),
    ("SKU-2001", 250.0, "Limited", 14),
    ("SKU-3001", 45.0, "Available", 5)
]
supplier_schema = T.StructType([
    T.StructField("sku_id", T.StringType(), True),
    T.StructField("cost_price", T.DoubleType(), True),
    T.StructField("availability", T.StringType(), True),
    T.StructField("lead_time_days", T.IntegerType(), True)
])
df_supplier = spark.createDataFrame(supplier_data, supplier_schema)

# -----------------------------
# 4. Clickstream data
# -----------------------------
click_data = [
    ("SKU-1001", 500, 50, 25),   # impressions, add_to_cart, conversions
    ("SKU-2001", 120, 10, 3),
    ("SKU-3001", 300, 40, 20)
]
click_schema = T.StructType([
    T.StructField("sku_id", T.StringType(), True),
    T.StructField("impressions", T.IntegerType(), True),
    T.StructField("add_to_cart", T.IntegerType(), True),
    T.StructField("conversions", T.IntegerType(), True)
])
df_clicks = spark.createDataFrame(click_data, click_schema)

# -----------------------------
# 5. Market data (competitor pricing)
# -----------------------------
market_data = [
    ("SKU-1001", 11.0),
    ("SKU-2001", 310.0),
    ("SKU-3001", 58.0)
]
market_schema = T.StructType([
    T.StructField("sku_id", T.StringType(), True),
    T.StructField("competitor_price", T.DoubleType(), True)
])
df_market = spark.createDataFrame(market_data, market_schema)

# -----------------------------
# 6. Join all data sources
# -----------------------------
df_combined = (
    df_txn
    .join(df_products, "sku_id", "left")
    .join(df_supplier, "sku_id", "left")
    .join(df_clicks, "sku_id", "left")
    .join(df_market, "sku_id", "left")
)

# Derived features
df_combined = (
    df_combined
    .withColumn("conv_rate", F.col("conversions") / F.col("impressions"))
    .withColumn("margin", (F.col("price_paid") - F.col("cost_price")) / F.col("cost_price"))
    .withColumn("price_delta_vs_competitor", F.col("price_paid") - F.col("competitor_price"))
)

# -----------------------------
# 7. Aggregate by SKU
# -----------------------------
df_agg = (
    df_combined
    .groupBy("sku_id", "category", "manufacturer")
    .agg(
        F.avg("price_paid").alias("avg_price"),
        F.sum("quantity").alias("units_sold"),
        F.sum("conversions").alias("total_purchases"),
        F.sum("impressions").alias("total_impressions"),
        F.first("cost_price").alias("cost_price"),
        F.first("competitor_price").alias("competitor_price"),
        F.first("fulfillment_method").alias("fulfillment_method")
    )
    .withColumn("conv_rate", F.col("total_purchases") / F.col("total_impressions"))
    .withColumn("margin", (F.col("avg_price") - F.col("cost_price")) / F.col("cost_price"))
)

# -----------------------------
# 8. Anchor detection
# -----------------------------
w = Window.partitionBy("category").orderBy(F.desc("units_sold"))
df_agg = (
    df_agg
    .withColumn("sales_rank", F.row_number().over(w))
    .withColumn("category_count", F.count("sku_id").over(Window.partitionBy("category")))
    .withColumn("is_anchor", F.col("sales_rank") <= (F.col("category_count") * 0.10))
)

# -----------------------------
# 9. Pricing logic (UDF)
# -----------------------------
def suggest_price(cost_price, current_price, competitor_price, conv_rate, category, is_anchor):
    min_margin = {
        "Consumables": 0.05,
        "Instruments": 0.20,
        "Reagents": 0.10
    }.get(category, 0.10)
    
    floor_price = cost_price * (1 + min_margin)
    
    if is_anchor:
        suggested = max(floor_price, min(current_price, competitor_price - 0.01))
        reason = "anchor_competitive"
    else:
        candidates = [current_price * (1 + p) for p in [-0.2, -0.1, 0, 0.1, 0.2]]
        best_price, best_profit = current_price, -1e9
        for p in candidates:
            if p < floor_price:
                continue
            price_change_pct = (p - current_price) / current_price
            est_conv = max(0.0, conv_rate * (1 - 1.0 * price_change_pct))
            exp_profit = (p - cost_price) * est_conv
            if exp_profit > best_profit:
                best_price, best_profit = p, exp_profit
        suggested = best_price
        reason = "profit_optimized"
    
    return float(round(suggested, 2)), reason

suggest_udf = F.udf(suggest_price, T.StructType([
    T.StructField("suggested_price", T.DoubleType()),
    T.StructField("reason", T.StringType())
]))

df_suggest = df_agg.withColumn(
    "pricing",
    suggest_udf(
        F.col("cost_price"),
        F.col("avg_price"),
        F.col("competitor_price"),
        F.col("conv_rate"),
        F.col("category"),
        F.col("is_anchor")
    )
)

df_suggest = (
    df_suggest
    .withColumn("suggested_price", F.col("pricing.suggested_price"))
    .withColumn("reason", F.col("pricing.reason"))
    .drop("pricing")
)

display(df_suggest)


# COMMAND ----------

# -----------------------------
# 10. Save results to FileStore (Parquet)
# -----------------------------
output_path = "dbfs:/FileStore/pricing_suggestions_parquet_v1"

(df_suggest.write
    .format("parquet")
    .mode("overwrite")
    .save(output_path))



# COMMAND ----------

