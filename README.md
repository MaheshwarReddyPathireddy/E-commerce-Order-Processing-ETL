# E-commerce-Order-Processing-ETL

# ShopFast ETL Pipeline

## Project Overview

In this project, I built an end-to-end PySpark ETL pipeline  for ShopFast, an e-commerce startup, to automate their daily sales reporting.

Here’s exactly what I implemented:

1.Extract:

 * Read MySQL tables: `customers`, `orders`, and `products` using JDBC.
 * Read CSV files from local desktop: `payments.csv` and `inventory.csv`.

2.Transform

  Handled duplicate column names (`status`) by renaming:

   * `orders.status → order_status`
   * `payments.status → payment_status`
   * Joined all datasets into a single fact_sales table.
   * Calculated additional metric,total_price = quantity × price.

3. Load

   * Wrote the consolidated fact_sales table back into MySQL for reporting.

Verification

Loaded the final table in PySpark to confirm all joins and calculations were correct.
