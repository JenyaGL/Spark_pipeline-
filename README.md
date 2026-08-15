End-to-End Real-Time Car Telemetry Monitoring Pipeline

This repository contains a complete, end-to-end real-time data engineering pipeline built with Apache Spark (PySpark) and Apache Kafka. The project simulates a fleet of vehicles streaming telemetry data (speed, RPM, gear), enriches that stream with static dimension data stored in S3, detects driving anomalies in real-time, and generates a live dashboard of aggregated alerts.

Technology Stack
Data Processing Engine: Apache Spark (PySpark Structured Streaming & Batch)

Message Broker: Apache Kafka

Object Storage: MinIO (S3-compatible storage)

Data Generator: Python

Data Formats: JSON (Streaming), Parquet (Static Dimensions)

Pipeline Architecture & Process Flow
The pipeline operates in three major phases: Batch Dimension Creation, Real-Time Data Generation, and Streaming Processing & Alerting.

Static Data Initialization: Batch PySpark jobs create reference tables (Dimensions) for Car Models, Colors, and a master list of 20 unique Cars. These are saved to S3 (s3a://spark/data/dims/).

Simulated Telemetry: A Python script acts as a fleet of cars, continuously pushing random sensor data (speed, RPM, gear) to a Kafka topic (sensors-sample) every second.

Stream Enrichment: A PySpark Structured Streaming job consumes the raw telemetry, joins it on-the-fly with the static S3 dimension tables, calculates expected metrics, and pushes the enriched data to a new Kafka topic (samples-enriched).

Alert Detection: A downstream PySpark streaming job evaluates the enriched stream against predefined safety rules (e.g., speeding). Violations are routed to an alert Kafka topic (alert-data).

Aggregation & Output: A final streaming consumer calculates running totals and maximums over a 15-minute sliding watermark, printing a live scoreboard to the console.

File Structure & Step-by-Step Explanation
Utilities & Configuration
spark_sessions_config.py: Initializes the SparkSession. It configures the connection to the Kafka cluster (course-kafka:9092) and sets up the Hadoop configurations needed to read/write from the S3/MinIO endpoint (http://minio:9000).

schemas.py: Contains the PySpark StructType definitions for the Kafka JSON payloads, ensuring data is strictly typed when parsed from the message broker.

Phase 1: Batch Dimension Creation (Source Data)
These scripts must be run once before starting the streaming pipeline. They represent Exercises 1, 2, and 3.

ex1_car_models.py

Process: Creates a static DataFrame of car models (e.g., Mazda 3, Toyota Corolla).

Destination: Saves as Parquet to s3a://spark/data/dims/car_models.

ex2_car_colors.py

Process: Creates a static DataFrame of car colors (Black, Red, Gray, etc.).

Destination: Saves as Parquet to s3a://spark/data/dims/car_colors.

ex3_cars.py

Process: Generates 20 unique cars, randomly assigning a 7-digit car_id, a 9-digit driver_id, and foreign keys (model_id, color_id) linked to the previous dimensions.

Destination: Saves as Parquet to s3a://spark/data/dims/cars.

Phase 2: Real-Time Telemetry Generation
ex4_data_generator.py

Process: A Python script utilizing a while True loop and time.sleep(1). It reads the 20 cars from S3 and generates a random sensor event for each car every second.

Event Schema: event_id, event_time, car_id, speed (0-200), rpm (0-8000), gear (1-7).

Destination: Publishes JSON payloads to the Kafka topic sensors-sample.

Phase 3: Streaming Processing & Aggregation
These scripts are long-running PySpark Structured Streaming applications.

ex5_data_enrichment.py

Process: Reads the raw JSON stream from sensors-sample. Performs a streaming-static join with the car_models, car_colors, and cars Parquet files from S3 to append driver_id, brand_name, model_name, and color_name. Calculates a new column: expected_gear = round(speed / 30).

Destination: Writes the enriched stream to the Kafka topic samples-enriched.

ex6_alerting_detection.py

Process: Subscribes to samples-enriched. Applies logical filters to detect hazardous driving.

Alert Conditions (OR): speed > 120 | rpm > 6000 | gear != expected_gear.

Destination: Writes the filtered anomaly stream to the Kafka topic alert-data.

ex7_alerting_counter.py

Process: Subscribes to alert-data. Converts event_time to a Timestamp and applies a 15-minute watermark to handle late-arriving data. Calculates global metrics: total rows, alert counts by car color, and absolute maximums for speed, gear, and RPM.

Destination: Writes the aggregated metrics to the Console using a 1-minute trigger in complete output mode.

How to Run the Pipeline
Start Infrastructure: Ensure your Kafka broker (course-kafka:9092) and MinIO server are running (typically via docker-compose up).

Generate Dimensions: Run ex1, ex2, and ex3 sequentially to populate your S3 buckets.

Start the Generator: Run ex4_data_generator.py in its own terminal window and leave it running to simulate live traffic.

Start the Streaming Processors:

Open a new terminal and run ex5_data_enrichment.py.

Open a new terminal and run ex6_alerting_detection.py.

View the Dashboard: Open a final terminal and run ex7_alerting_counter.py. Within one minute, you will see the live aggregated metrics printing to your screen.
