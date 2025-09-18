
# Dynamic Product Pricing Prototype

This repository contains a prototype implementation of **Dynamic Product Pricing** for a B2B e-commerce platform (life sciences & medical supplies).  
The focus is on **system design**, **data processing pipeline**, and a **rule-based pricing engine**.  
No machine learning model is required — the logic is transparent, explainable, and designed for rapid prototyping.

---

## 📝 Summary of the Solution

- **Data Sources (synthetic)**:  
  Product metadata, transaction history, supplier data, clickstream events, and competitor pricing.  
- **Pipeline**:
  1. Ingest data into Spark DataFrames (simulating batch loads).  
  2. Join and clean into a unified dataset.  
  3. Derive features: margin, conversion rate, anchor product flags.  
  4. Apply rule-based pricing logic:  
     - Anchor SKUs → competitive pricing (stay close to market price).  
     - Non-anchor SKUs → profit-optimized pricing (maximize margin).  
  5. Save results in **Parquet** format to `dbfs:/FileStore/pricing_suggestions_parquet_v1`.  
- **Outputs**: A table of SKUs with average price, suggested price, anchor flag, margin, and reasoning.  
- **Experimentation Ready**: Design supports logging and A/B testing toggles for future experiments.  

---

## ⚙️ Setup Instructions

1. Open **Databricks Community Edition** (or any Spark environment).  
2. Import the notebook from this repository (`pricing_notebook.py` or `.dbc` export).  
3. Ensure your cluster uses **Python 3.x** and **Spark 3.x**.  
4. No external data sources are required — the notebook generates synthetic data for demo purposes.  

---

## ▶️ How to Run the Pipeline

1. Run all cells in order inside the Databricks notebook.  
2. The notebook will:  
   - Create synthetic datasets for product, transaction, supplier, clickstream, and market data.  
   - Join them into a single enriched dataset.  
   - Compute derived features (conversion rate, margin, anchor detection).  
   - Apply rule-based pricing logic.  
   - Save results to:  
     ```
     databricks
     ```
   - Read results back and display as a table.  

3. Final outputs include:  
   - SKU ID  
   - Category  
   - Average Price  
   - Suggested Price  
   - Anchor Flag  
   - Margin  
   - Reason for pricing decision  

---

## 💡 Example Output

| sku_id   | category     | avg_price | suggested_price | is_anchor | margin | reason             |
|----------|-------------|-----------|-----------------|-----------|--------|--------------------|
| SKU-1001 | Consumables | 11.75     | 11.0            | true      | 0.30   | anchor_competitive |
| SKU-2001 | Instruments | 320.0     | 310.0           | true      | 0.28   | anchor_competitive |
| SKU-3001 | Reagents    | 60.0      | 66.0            | false     | 0.33   | profit_optimized   |

---

## 🧑‍💻 Code Style

- Code is structured into sections:
  - Data creation
  - Joins & feature engineering
  - Anchor detection
  - Pricing logic (with a clear UDF)
  - Output save + read back
- Each step is **clearly commented** for readability.  
- Pricing logic is encapsulated in a simple **Python UDF** with reasoning labels.  

---

## 🤖 AI-Assisted Tools Used

- **ChatGPT**:  
  - Drafted the initial system design text (for PDF submission).  
  - Generated scaffolding for PySpark code in Databricks.  
  - Helped debug write errors (`Delta` vs `Parquet`).  
  - Polished this README for clarity.  

- **Databricks CE**:  
  - Used for running Spark pipelines and persisting Parquet outputs.  

---

