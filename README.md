# Sales Trend and Return Rate Analysis using PySpark

## 📊 Project Overview

This project performs a comprehensive analysis of a large-scale e-commerce dataset using PySpark. The primary goal is to extract meaningful business insights on product sales, customer behavior, and return rates to support data-driven decision-making. The pipeline involves data cleaning, transformation, aggregation, and visualization of key performance indicators (KPIs).

## 📝 Problem Statement

As a Business Analyst for a large e-commerce platform, the objective is to analyze transaction data to:
1.  Perform data cleaning and transformation on large datasets using PySpark.
2.  Calculate and generate core business KPIs.
3.  Produce insightful summaries to guide business strategy.

## ⚙️ Tech Stack

- **Data Processing:** Apache Spark (PySpark)
- **Language:** Python
- **Libraries:** Pandas (for data generation and visualization hand-off), Faker (for mock data), Matplotlib & Seaborn (for visualization)
- **Environment:** Google Colab / Local Spark setup

## 📂 Dataset

The analysis is performed on three mock CSV files representing a real-world e-commerce database:

-   `orders.csv`: Contains transactional data (`order_id`, `user_id`, `product_id`, `order_date`, `price`, `quantity`, `is_returned`).
-   `products.csv`: Contains product details (`product_id`, `name`, `category`).
-   `users.csv`: Contains user information (`user_id`, `location`, `signup_date`).

A Python script is included to generate over 100,000 sample records for a realistic simulation.

## 🚀 Core Analysis & Insights

The PySpark script calculates the following key business metrics:

1.  **Total Revenue by Category:** Identifies which product categories are the most profitable.
2.  **Monthly Sales Trend:** Tracks revenue performance over time to spot seasonal trends and growth patterns.
3.  **Return Rate by Category:** Pinpoints categories with high return rates, which may indicate issues with product quality or descriptions.
4.  **Top 5 Users by Spend:** Highlights the most valuable customers for loyalty programs and targeted marketing.
5.  **Category with Highest Return Rate:** Quickly flags the most problematic product category.

![Monthly Sales Trend](https://i.imgur.com/u7qFk9a.png)
*(Sample visualization generated from the analysis)*

## 🛠️ How to Run

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/pyspark-ecommerce-analysis.git](https://github.com/your-username/pyspark-ecommerce-analysis.git)
    cd pyspark-ecommerce-analysis
    ```

2.  **Install dependencies:**
    It is recommended to use a virtual environment.
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: `requirements.txt` should contain `pyspark`, `pandas`, `faker`, `matplotlib`, and `seaborn`)*

3.  **Execute the script:**
    Run the main Python script which generates data and performs the analysis.
    ```bash
    python main_analysis.py
    ```
    The script will print the results of the analysis to the console and display a plot of the monthly sales trend.
