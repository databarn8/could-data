# Kafka-Databricks Integration Setup

This guide contains all the files and scripts needed for setting up Kafka with Docker and connecting it to Databricks FREE edition using ngrok.

## Project Structure

Create this folder structure on your Windows machine:

```
C:\kafka-databricks-project\
├── docker-compose.yml
├── docker-compose-external.yml
├── scripts\
│   ├── kafka_producer.py
│   ├── test_kafka.py
│   └── setup.bat
├── databricks\
│   └── kafka_integration_notebook.py
└── README.md
```

## File Contents

### 1. docker-compose.yml
**Location**: `C:\kafka-databricks-project\docker-compose.yml`

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: kafka-topics --bootstrap-server kafka:29092 --list
      interval: 30s
      timeout: 10s
      retries: 3

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:
```

### 2. docker-compose-external.yml
**Location**: `C:\kafka-databricks-project\docker-compose-external.yml`

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: kafka-topics --bootstrap-server kafka:29092 --list
      interval: 30s
      timeout: 10s
      retries: 3

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:

# Note: Update KAFKA_ADVERTISED_LISTENERS with your ngrok URL
# Example: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://your-ngrok-url:port
```

### 3. scripts/kafka_producer.py
**Location**: `C:\kafka-databricks-project\scripts\kafka_producer.py`

```python
"""
Kafka Producer Script for Databricks Integration
Run this script locally to send continuous data to Kafka
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Local Kafka
TOPIC_NAME = 'databricks-topic'

# Sample data generators
EVENTS = ['login', 'logout', 'purchase', 'view_product', 'signup', 'cart_add']
PRODUCTS = ['PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005']
USER_IDS = list(range(1, 101))  # 100 users

def generate_event():
    """Generate a random event"""
    event_type = random.choice(EVENTS)
    user_id = random.choice(USER_IDS)
    
    base_event = {
        'user_id': user_id,
        'event': event_type,
        'timestamp': datetime.now().isoformat(),
        'session_id': f'session_{random.randint(1000, 9999)}'
    }
    
    # Add event-specific data
    if event_type == 'purchase':
        base_event['amount'] = round(random.uniform(10.0, 200.0), 2)
        base_event['product_id'] = random.choice(PRODUCTS)
    elif event_type == 'view_product':
        base_event['product_id'] = random.choice(PRODUCTS)
        base_event['view_duration'] = random.randint(5, 300)  # seconds
    elif event_type == 'cart_add':
        base_event['product_id'] = random.choice(PRODUCTS)
        base_event['quantity'] = random.randint(1, 5)
    
    return base_event

def main():
    """Main producer function"""
    print(f"🚀 Starting Kafka producer...")
    print(f"📡 Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📝 Topic: {TOPIC_NAME}")
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    
    try:
        message_count = 0
        while True:
            # Generate and send event
            event = generate_event()
            key = str(event['user_id'])
            
            # Send to Kafka
            future = producer.send(TOPIC_NAME, key=key, value=event)
            
            # Wait for send confirmation (optional)
            try:
                record_metadata = future.get(timeout=10)
                message_count += 1
                print(f"✅ Message {message_count}: {event['event']} by user {event['user_id']} "
                      f"-> Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            except Exception as e:
                print(f"❌ Failed to send message: {e}")
            
            # Wait before sending next message
            time.sleep(random.uniform(1, 3))  # 1-3 seconds between messages
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    except Exception as e:
        print(f"❌ Producer error: {e}")
    finally:
        producer.close()
        print("✅ Producer closed successfully")

def send_batch_messages(num_messages=10):
    """Send a batch of messages for testing"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    
    print(f"📤 Sending {num_messages} test messages...")
    
    for i in range(num_messages):
        event = generate_event()
        key = str(event['user_id'])
        producer.send(TOPIC_NAME, key=key, value=event)
        print(f"  📨 Sent message {i+1}: {event['event']}")
    
    producer.flush()  # Ensure all messages are sent
    producer.close()
    print("✅ Batch messages sent successfully!")

if __name__ == "__main__":
    # Install required package first: pip install kafka-python
    try:
        main()
    except ImportError:
        print("❌ kafka-python package not found!")
        print("📦 Install it using: pip install kafka-python")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# Uncomment to send a batch of test messages instead
# send_batch_messages(20)
```

### 4. scripts/test_kafka.py
**Location**: `C:\kafka-databricks-project\scripts\test_kafka.py`

```python
"""
Simple script to test Kafka connection and send test messages
"""

import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

def test_kafka_connection():
    """Test basic Kafka connection"""
    try:
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send test message
        test_message = {"test": "Hello Kafka!", "timestamp": "2024-01-01T00:00:00"}
        future = producer.send('test-topic', test_message)
        result = future.get(timeout=10)
        
        print(f"✅ Message sent successfully!")
        print(f"   Topic: {result.topic}")
        print(f"   Partition: {result.partition}")
        print(f"   Offset: {result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False

def list_topics():
    """List all Kafka topics"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        topics = admin_client.list_topics()
        print(f"📋 Available topics: {list(topics)}")
        return topics
    except Exception as e:
        print(f"❌ Failed to list topics: {e}")
        return []

def create_topic(topic_name, num_partitions=3, replication_factor=1):
    """Create a Kafka topic"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        admin_client.create_topics([topic])
        print(f"✅ Topic '{topic_name}' created successfully!")
    except Exception as e:
        print(f"❌ Failed to create topic: {e}")

if __name__ == "__main__":
    print("🔍 Testing Kafka connection...")
    
    # Test connection
    if test_kafka_connection():
        print("\n📋 Listing topics...")
        list_topics()
        
        print("\n🆕 Creating test topic...")
        create_topic("databricks-topic")
        
        print("\n✅ Kafka test completed successfully!")
    else:
        print("\n❌ Kafka test failed! Check if Kafka is running.")
```

### 5. scripts/setup.bat
**Location**: `C:\kafka-databricks-project\scripts\setup.bat`

```batch
@echo off
echo 🚀 Kafka-Databricks Setup Script
echo ================================

echo 📦 Installing Python dependencies...
pip install kafka-python

echo 🐳 Starting Docker Compose services...
cd /d "%~dp0\.."
docker-compose up -d

echo ⏳ Waiting for services to start...
timeout /t 30

echo 🔍 Testing Kafka connection...
python scripts\test_kafka.py

echo ✅ Setup completed!
echo 📋 Next steps:
echo    1. Start ngrok: ngrok tcp 9092
echo    2. Update docker-compose-external.yml with ngrok URL
echo    3. Restart with external config: docker-compose -f docker-compose-external.yml up -d
echo    4. Run producer: python scripts\kafka_producer.py

pause
```

### 6. databricks/kafka_integration_notebook.py
**Location**: `C:\kafka-databricks-project\databricks\kafka_integration_notebook.py`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka Integration with Databricks FREE Edition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC Update the KAFKA_BOOTSTRAP_SERVERS with your ngrok URL

# COMMAND ----------

# Configuration
# Replace with your ngrok URL (without tcp://)
KAFKA_BOOTSTRAP_SERVERS = "0.tcp.ngrok.io:12345"  # Replace with your ngrok URL
TOPIC_NAME = "databricks-topic"

print(f"🔗 Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"📝 Topic Name: {TOPIC_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Test Kafka Connection

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

try:
    # Create a simple DataFrame to test connection
    test_df = spark.range(1).select(
        lit("test-key").alias("key"),
        lit("Hello from Databricks!").alias("value")
    )
    
    # Try to write to Kafka (this will create the topic if it doesn't exist)
    test_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", TOPIC_NAME) \
        .save()
    
    print("✅ Successfully connected to Kafka!")
    
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Send Sample Data to Kafka

# COMMAND ----------

import json

# Generate sample streaming data
sample_data = [
    {"user_id": 1, "event": "login", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": 2, "event": "purchase", "timestamp": "2024-01-01T10:05:00", "amount": 29.99},
    {"user_id": 1, "event": "logout", "timestamp": "2024-01-01T10:30:00"},
    {"user_id": 3, "event": "signup", "timestamp": "2024-01-01T11:00:00"},
    {"user_id": 2, "event": "view_product", "timestamp": "2024-01-01T11:15:00", "product_id": "ABC123"}
]

# Convert to DataFrame and send to Kafka
sample_df = spark.createDataFrame([
    (str(row["user_id"]), json.dumps(row)) for row in sample_data
], ["key", "value"])

sample_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", TOPIC_NAME) \
    .save()

print(f"✅ Sent {len(sample_data)} messages to topic '{TOPIC_NAME}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Read from Kafka (Batch Mode)

# COMMAND ----------

print("📖 Reading messages from Kafka (batch mode):")

batch_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert binary data to string and parse JSON
parsed_batch_df = batch_df.select(
    col("key").cast("string").alias("user_id"),
    col("value").cast("string").alias("json_data"),
    col("timestamp").alias("kafka_timestamp"),
    col("partition"),
    col("offset")
)

parsed_batch_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Parse JSON Data and Create Structured DataFrame

# COMMAND ----------

from pyspark.sql.functions import from_json

# Define schema for the JSON data
json_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("event", StringType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("product_id", StringType())
])

# Parse JSON and expand columns
final_df = parsed_batch_df.select(
    col("user_id"),
    from_json(col("json_data"), json_schema).alias("data"),
    col("kafka_timestamp"),
    col("partition"),
    col("offset")
).select(
    col("user_id"),
    col("data.*"),
    col("kafka_timestamp"),
    col("partition"),
    col("offset")
)

print("🔍 Parsed data:")
final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Streaming Processing Setup

# COMMAND ----------

print("🚀 Setting up streaming query...")

# Read stream from Kafka
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# Process streaming data
processed_stream = streaming_df.select(
    col("key").cast("string").alias("user_id"),
    from_json(col("value").cast("string"), json_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select(
    col("user_id"),
    col("data.*"),
    col("kafka_timestamp")
)

# Write stream to console (for demonstration)
query = processed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

print("📡 Streaming query started! It will show new messages every 10 seconds.")
print("🛑 Run query.stop() in the next cell to stop the streaming query.")

# COMMAND ----------

# Uncomment to stop the streaming query
# query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Save Data and Run Analytics

# COMMAND ----------

print("💾 Creating temporary view for analytics...")

# Create temporary view
final_df.createOrReplaceTempView("kafka_events")

# Display sample data
spark.sql("SELECT * FROM kafka_events LIMIT 10").show()

# COMMAND ----------

print("📊 Running analytics on Kafka data...")

# Event count by type
event_counts = spark.sql("""
    SELECT event, COUNT(*) as count
    FROM kafka_events
    GROUP BY event
    ORDER BY count DESC
""")

print("Event counts:")
event_counts.show()

# Purchase analytics (if any purchase events exist)
purchase_analytics = spark.sql("""
    SELECT 
        COUNT(*) as total_purchases,
        AVG(amount) as avg_amount,
        SUM(amount) as total_revenue
    FROM kafka_events
    WHERE event = 'purchase' AND amount IS NOT NULL
""")

print("Purchase analytics:")
purchase_analytics.show()

# COMMAND ----------

print("🎉 Kafka integration completed successfully!")
print("📋 Summary:")
print(f"   - Connected to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"   - Topic: {TOPIC_NAME}")
print(f"   - Messages processed: {final_df.count()}")
print("   - Analytics completed")
```

### 7. README.md
**Location**: `C:\kafka-databricks-project\README.md`

```markdown
# Kafka-Databricks Integration Project

This project sets up Apache Kafka using Docker and connects it to Databricks FREE edition using ngrok for external access.

## Prerequisites

- Docker Desktop for Windows
- Python 3.7+
- ngrok account (free)
- Databricks FREE edition account

## Quick Start

1. **Clone/Download Project**
   - Create folder: `C:\kafka-databricks-project\`
   - Copy all files to this folder

2. **Install Python Dependencies**
   ```cmd
   pip install kafka-python
   ```

3. **Start Kafka Services**
   ```cmd
   docker-compose up -d
   ```

4. **Test Local Setup**
   ```cmd
   python scripts\test_kafka.py
   ```

5. **Setup ngrok**
   ```cmd
   ngrok authtoken YOUR_TOKEN
   ngrok tcp 9092
   ```

6. **Update External Configuration**
   - Edit `docker-compose-external.yml`
   - Replace ngrok URL in KAFKA_ADVERTISED_LISTENERS

7. **Restart with External Config**
   ```cmd
   docker-compose down
   docker-compose -f docker-compose-external.yml up -d
   ```

8. **Setup Databricks**
   - Install Maven library: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0`
   - Import notebook from `databricks/kafka_integration_notebook.py`
   - Update KAFKA_BOOTSTRAP_SERVERS with ngrok URL

9. **Start Producer (Optional)**
   ```cmd
   python scripts\kafka_producer.py
   ```

## File Descriptions

- `docker-compose.yml`: Local Kafka setup
- `docker-compose-external.yml`: Kafka with external access
- `scripts/kafka_producer.py`: Continuous data producer
- `scripts/test_kafka.py`: Connection testing script
- `scripts/setup.bat`: Automated setup script
- `databricks/kafka_integration_notebook.py`: Databricks notebook

## Useful Commands

```bash
# View Kafka UI
http://localhost:8080

# Check running containers
docker-compose ps

# View Kafka logs
docker logs kafka

# Stop all services
docker-compose down

# Remove all data
docker-compose down -v
```

## Troubleshooting

1. **Port conflicts**: Change ports in docker-compose.yml
2. **ngrok connection**: Restart ngrok and update configuration
3. **Databricks connection**: Check cluster status and library installation
4. **Memory issues**: Restart Docker Desktop

## Support

For issues, check the logs:
- Kafka: `docker logs kafka`
- Zookeeper: `docker logs zookeeper`
- Producer: Check terminal output
```

## Instructions for You

1. **Create the folder structure** on your machine as shown above
2. **Copy each file** to its respective location
3. **Follow the setup steps** in the README.md
4. **Run the setup.bat** script to automate the initial setup

All the scripts are organized and ready to use. The setup should work seamlessly once you have Docker Desktop and ngrok installed on your Windows machine.

*************************************************************************************************************************************************
Great! Let's set up a dockerized Kafka server and use ngrok to expose it to the internet for Databricks FREE edition. Here's a step-by-step guide:
Step 1: Install Prerequisites
Docker Desktop for Windows

Download Docker Desktop from https://www.docker.com/products/docker-desktop/
Install and start Docker Desktop
Verify installation:
cmddocker --version
docker-compose --version


Install ngrok

Download ngrok from https://ngrok.com/download
Extract to a folder (e.g., C:\ngrok\)
Sign up for a free ngrok account
Get your authtoken from the dashboard
Configure ngrok:
cmdC:\ngrok\ngrok.exe authtoken YOUR_AUTH_TOKEN


Step 2: Create Docker Compose for Kafka
Create a docker-compose.yml file:Docker Compose for Kafka SetupCode version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
Step 3: Start Kafka with Docker

Save the docker-compose.yml file in a folder (e.g., C:\kafka-docker\)
Start Kafka services:
cmdcd C:\kafka-docker
docker-compose up -d

Verify services are running:
cmddocker-compose ps

Check Kafka UI at http://localhost:8080 (optional but helpful for monitoring)

Step 4: Test Local Kafka Setup
Create a test topic and verify it works:
cmd# Create a topic
docker exec kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1

# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:29092

# Produce test messages
docker exec -it kafka kafka-console-producer --topic test-topic --bootstrap-server localhost:29092

# In another terminal, consume messages
docker exec -it kafka kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:29092
Step 5: Expose Kafka with ngrok

Start ngrok to expose Kafka port:
cmdC:\ngrok\ngrok.exe tcp 9092

Note the forwarding URL (e.g., tcp://0.tcp.ngrok.io:12345)
Update Kafka configuration for external access. Create a new docker-compose-external.yml:
Docker Compose for External Kafka AccessCode version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
Important: Replace 0.tcp.ngrok.io:12345 with your actual ngrok URL from step 2.

Restart with external configuration:
cmddocker-compose down
docker-compose -f docker-compose-external.yml up -d


Step 6: Configure Databricks FREE Edition
Install Kafka Libraries

In Databricks, go to your cluster
Click "Libraries" → "Install New"
Choose "Maven" and enter:
org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0

Install the library and restart the cluster

Create Databricks NotebookDatabricks Kafka Integration NotebookCode # Databricks Kafka Integration Notebook

# STEP 1: Configuration
# Replace with your ngrok URL (without tcp://)
KAFKA_BOOTSTRAP_SERVERS = "0.tcp.ngrok.io:12345"  # Replace with your ngrok URL
TOPIC_NAME = "databricks-topic"

# STEP 2: Test Kafka ConnStep 7: Python Producer Script (Optional)
Create a Python script to continuously send data to Kafka:Python Kafka Producer ScriptCode """
Kafka Producer Script for Databricks Integration
Run this script locally to send continuous data to Kafka
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SEStep 8: Complete Setup Process
Order of Operations:

Start Docker services:
cmdcd C:\kafka-docker
docker-compose up -d

Start ngrok:
cmdC:\ngrok\ngrok.exe tcp 9092

Update the external compose file with your ngrok URL
Restart Kafka with external configuration:
cmddocker-compose down
docker-compose -f docker-compose-external.yml up -d

Install Kafka library in Databricks cluster
Update the Databricks notebook with your ngrok URL
Run the notebook to test the connection

Producer Script Setup (Optional):
cmd# Install Python Kafka client
pip install kafka-python

# Run the producer script
python kafka_producer.py
Important Notes for Databricks FREE Edition:

Cluster limitations: FREE edition has limited compute resources
Library installations: You can install Maven libraries but with some restrictions
Networking: ngrok provides the external access needed
Storage: Use temporary views or write to DBFS for data persistence

Troubleshooting:

ngrok tunnel closed: Restart ngrok and update the Kafka configuration
Connection timeout: Check Windows Firewall and Docker network settings
Library conflicts: Ensure Spark version compatibility with Kafka connector
Memory issues: Restart the Databricks cluster if it becomes unresponsive

This setup gives you a fully functional Kafka environment accessible from Databricks FREE edition through ngrok tunneling!
