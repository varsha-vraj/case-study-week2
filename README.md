<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Ecommerce Data Vault 🚀</title>
  <style>
    body {
      font-family: 'Segoe UI', sans-serif;
      background-color: #fdfdfd;
      padding: 2em;
      line-height: 1.6;
    }
    h1 {
      color: #2e86de;
    }
    h2 {
      color: #27ae60;
    }
    code {
      background: #f4f4f4;
      padding: 2px 4px;
      border-radius: 4px;
    }
    pre {
      background: #f4f4f4;
      padding: 10px;
      overflow-x: auto;
      border-radius: 6px;
    }
    img {
      max-width: 100%;
      border-radius: 8px;
      margin: 10px 0;
      box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
  </style>
</head>
<body>

  <h1>📦 Ecommerce Data Vault Project 🚀</h1>

  <p>
    This project showcases a complete end-to-end data warehouse architecture using <strong>Data Vault 2.0</strong>, 
    <strong>Delta Lake</strong>, and <strong>PySpark</strong>. Built as part of a real-world case study to transform raw ecommerce data into a scalable, auditable, and analytics-ready platform.
  </p>

  <h2>🧰 Tech Stack</h2>
  <ul>
    <li>🔥 Apache Spark (PySpark)</li>
    <li>🧊 Delta Lake for ACID + versioning</li>
    <li>🐍 Python with virtualenv</li>
    <li>📁 Local CSVs as data source</li>
    <li>🧪 Delta JARs for local Spark execution</li>
    <li>📊 Designed for BI tools (Power BI, Tableau)</li>
  </ul>

  <h2>📁 Project Structure</h2>
  <pre>
ecommerce-data-vault/
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
├── src/
│   ├── hub_*.py, link_*.py, sat_*.py
│   ├── scd_type2_*.py, pit_tables.py
│   ├── dim_customer.py, dim_product.py, fact_order.py
├── delta_tables/
│   ├── hub_*, sat_*, link_*, scd_*, pit_*, dim_*, fact_*
└── README.html
  </pre>

  <h2>🧠 Workflow Overview</h2>
  <ol>
    <li>🔽 Load raw CSV data from the <code>data/</code> folder.</li>
    <li>🏗️ Create Data Vault model using Hubs, Links, Satellites.</li>
    <li>📜 Track history using SCD Type 2 with audit columns.</li>
    <li>📌 Generate PIT Tables to simplify latest joins.</li>
    <li>🌟 Transform to Star Schema with surrogate keys.</li>
    <li>📊 Connect to BI tools for insights.</li>
  </ol>

  <h2>📊 ER Diagrams</h2>
  <p><strong>Data Vault ERD:</strong></p>
  <img src="https://raw.githubusercontent.com/your-username/your-repo/main/docs/datavault_erd.png" alt="Data Vault ERD">

  <p><strong>Star Schema ERD:</strong></p>
  <img src="https://raw.githubusercontent.com/your-username/your-repo/main/docs/star_erd.png" alt="Star Schema ERD">

  <h2>🏁 Final Tables</h2>
  <ul>
    <li>🔷 Hubs: <code>hub_customer</code>, <code>hub_product</code>, <code>hub_order</code></li>
    <li>🔗 Links: <code>link_order_product</code>, <code>link_order_customer</code></li>
    <li>📑 Satellites: <code>sat_customer</code>, <code>sat_product</code>, <code>sat_order</code></li>
    <li>⏳ SCD Type 2: <code>scd_sat_customer</code>, <code>scd_sat_product</code>, <code>scd_sat_order</code></li>
    <li>📌 PIT: <code>pit_customer</code>, <code>pit_product</code>, <code>pit_order</code></li>
    <li>⭐ Star Schema: <code>dim_customer</code>, <code>dim_product</code>, <code>fact_order</code></li>
  </ul>

  <h2>✅ Example Spark Submit Command</h2>
  <pre>
spark-submit ^
  --jars delta-core_2.12-2.4.0.jar,delta-storage-2.4.0.jar ^
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" ^
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" ^
  --conf spark.pyspark.python="venv/Scripts/python.exe" ^
  src/dim_customer.py
  </pre>

  <h2>👩‍💻 Author</h2>
  <p>
    <br>Email: <a href="mailto:varshaa112003@gmail.com">varshaa112003@gmail.com</a>
  </p>

</body>
</html>
