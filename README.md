# Real-Time-Data-Analysis-with-AWS-Snowflake-and-NiFi
This project demonstrates the implementation of a real-time data streaming pipeline using AWS, Snowflake, and NiFi. By generating fake data, processing it in real-time with NiFi, and storing it in Snowflake for analysis


## Technologies Used

- **AWS**: Amazon Web Services provides cloud computing services for storing data and running applications.
- **Snowflake**: A cloud-based data warehousing platform for data storage and analysis.
- **Apache NiFi**: An open-source data integration platform that facilitates the automation of data flow between systems.
- **Docker**: Used for containerization of applications and services for easy deployment.
- **Python Faker Library**: Generates fake data for testing and development purposes.

## Pipeline Overview

1. **Data Generation**:
   - Use the Python Faker library to generate fake data for testing and development purposes.
   
2. **Data Storage**:
   - Store the generated data in AWS S3.
   
3. **Data Processing**:
   - Use Snowpipe, Stream, Task to automatically ingest the data from S3 into Snowflake .
   
4. **Data Analysis**:
   - Analyze the ingested data in Snowflake to gain real-time insights.


## Project Execution Flow

### EC2 Setup

1. **Connect to EC2 instance using SSH**.
2. **Copy project files to EC2 instance**.

### Docker Installation

1. **Install Docker on the EC2 instance**.
2. **Install Docker Compose** for managing multi-container Docker applications.

### Generate Fake Data

1. **Use Python Faker library to generate fake data**.
2. **Continuously generate and store fake data in CSV format**.
    - Refer to the script: [`generate_data.py`](./data_generation/generate_data.py)

### Snowflake Setup

1. **Create tables** for storing raw data, historical data, and updates.
2. **Create streams** for capturing data changes.
3. **Set up storage integration and stages** for data loading from S3.
4. **Create Snowpipe** for automatic ingestion of data from S3 into Snowflake.

### NiFi Configuration

1. **Set up NiFi** to integrate data flow between S3 and Snowflake.
2. **Configure processors** for reading data from S3 and loading it into Snowflake.

### Automate Data Processing with Tasks

1. **Create procedures** for handling data updates and inserts.
2. **Create tasks** to automate data processing at specified intervals.

### Data Analysis

1. **Analyze the data stored in Snowflake** for real-time insights.
2. **Utilize Snowflake features** for data querying, manipulation, and visualization.

## Getting Started

### Prerequisites

- AWS account with S3 and IAM setup.
- Snowflake account.
- Docker installed on your local machine.
- Apache NiFi setup.
- Python installed with Faker library.

### Setup Instructions

1. **Generate Fake Data**:
   - Install the Faker library: `pip install faker`
   - Create a Python script to generate and store fake data in S3.
   - Refer to the script: [`generate_data.py`](./data_generation/generate_data.py)

2. **Configure Snowpipe**:
   - Set up Snowpipe in Snowflake to automatically ingest data from your S3 bucket.

3. **Set Up Apache NiFi**:
   - Use NiFi to automate the data flow from S3 to Snowflake.
   - Configure NiFi using the provided configuration file: [`nifi_flow.xml`](./nifi/nifi_flow.xml)

4. **Run Docker Containers**:
   - Use Docker to deploy your applications and services.
   - Refer to the Docker setup: [`Dockerfile`](./docker/Dockerfile)

5. **Analyze Data in Snowflake**:
   - Use SQL queries in Snowflake to analyze the ingested data for real-time insights.

## Project Structure

```plaintext
.
├── data_generation
│   └── generate_data.py
├── docker
│   └── Dockerfile
├── nifi
│   └── nifi_flow.xml
├── README.md
└── requirements.txt
```

Conclusion
This project demonstrates the end-to-end process of real-time data analysis using AWS, Snowflake, and NiFi. By integrating these technologies, organizations can efficiently manage and analyze large volumes of data, enabling informed decision-making and insights generation. The automation capabilities provided by Snowflake tasks and NiFi streamline the data processing pipeline, enhancing efficiency and scalability.
