# Scalable Data Pipelines for Consumer Segmentation Application

## Overview
This project demonstrates the design and implementation of scalable, efficient, and reliable data pipelines for a consumer segmentation application. The pipelines are designed to handle data ingestion, transformation, storage, and retrieval, supporting both real-time and batch processing scenarios.

## Objective
Build a robust system to simulate, process, and analyze consumer-related events. The solution leverages modern data engineering tools to ensure data is ingested and processed reliably for downstream analytics and reporting.

## Pipeline Components

### Task 1: Kafka Producer Simulation
- **Description:**  
  Develop a Python-based Kafka producer to simulate user activity events. Events include:
  - **Movies Events:**  
    - Batch process a `Movies.txt` file using Apache Spark.
    - Extract and format movie-related data (e.g., parsing `ListPrice` to extract the dollar value).
    - Produce events with `event_name` set to `"movies_catalog_enriched"`.
  - **User Registration Events:**  
    - Include fields such as `timestamp` (ISO 8601 format), `event_name` ("consumer_registration"), `user_id` (unique, randomly generated), `age`, `masked_email`, and `preferred_language`.
  - **Sign In / Sign Out Events:**  
    - Each event includes a timestamp, event type (`"sign_in"` or `"sign_out"`), and associated `user_id`.
  - **Item View, Add to Cart, and Checkout Events:**  
    - Each event contains details such as `timestamp`, specific `event_name`, `item_id` (from Movies.txt), `user_id`, and additional identifiers (e.g., `cart_id`, `payment_method`).

- **Key Features:**
  - **OOP Implementation:** Utilizes object-oriented principles (encapsulation, inheritance, and polymorphism).
  - **Schema Validation:** Leverages Avro-formatted messages with Schema Registry integration.
  - **Randomized Event Generation:** Simulates realistic user interactions.

### Task 2: Real-Time Data Processing with Spark Structured Streaming & Snowflake Integration
- **Description:**  
  Use Apache Spark Structured Streaming to:
  - **Consume Kafka Topics:**  
    - Ingest events from topics such as `consumer_registration_topic`, `sign_in_topic`, `item_view_topic`, `added_to_cart`, `checkout_to_cart`, and `movies_catalog_enriched`.
  - **Data Transformation:**  
    - Apply necessary transformations and data cleaning (e.g., parsing prices and aligning field formats).
  - **Data Loading:**  
    - Use the Spark Snowflake Connector to load transformed data into Snowflake for further analysis and reporting.
  
- **Key Features:**
  - Supports micro-batch processing.
  - Ensures data consistency between Kafka messages and target Snowflake tables.

### Task 3: Orchestrate Movie File Processing with Apache Airflow
- **Description:**  
  Create an Apache Airflow DAG to manage the end-to-end processing of movie files:
  - **File Ingestion:**  
    - Triggered on a schedule or upon the arrival of new files.
  - **Processing:**  
    - Use Apache Spark to extract movie details from the `Movies.txt` file.
  - **Integration:**  
    - Ensure that after processing, movie-related events are produced to Kafka for real-time ingestion.
  
- **Key Features:**
  - Seamless orchestration and monitoring of file processing workflows.
  - Integration with the Kafka producer for continuous event generation.

### Task 5: User Registration Count Aggregation with Windowing
- **Description:**  
  Utilize Spark Structured Streaming to compute user registration metrics:
  - **Tumbling Window Aggregation:**  
    - Count distinct user registrations every hour over fixed, non-overlapping intervals.
  - **Sliding Window Aggregation:**  
    - Compute a rolling count of unique user registrations over a 1-hour period, updated every 15 minutes.
  - **Metrics Storage:**  
    - Write aggregated metrics (including timestamp, window type, and registration count) to Snowflake.

- **Bonus (Optional):**  
  - **Active Sessions Count:**  
    - Track real-time active sessions based on sign-in and sign-out events.
  - **Average Session Duration:**  
    - Calculate and store the average duration of active sessions in real time.

### Task 6: Total Sales Calculation of Movies
- **Description:**  
  Aggregate movie sales using Spark Structured Streaming:
  - **Aggregation:**  
    - Sum the `purchase_amount` from checkout events grouped by `movie_id`.
  - **Storage:**  
    - Write total sales metrics (including movie identifier, aggregated sales, and aggregation timestamp) to Snowflake.
  - **Performance:**  
    - Optimize the streaming query using stateful operations for efficient real-time computation.

## Technologies Used
- **Programming Language:** Python
- **Message Broker:** Apache Kafka (using the `confluent-kafka` library)
- **Data Processing:** Apache Spark (including Spark Structured Streaming)
- **Data Warehouse:** Snowflake
- **Orchestration:** Apache Airflow
- **Data Format:** Avro (with Schema Registry integration)

## Project Structure
