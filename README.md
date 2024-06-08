# Real-Time Data Stream Pipeline with AWS, Snowflake, and Apache NiFi

This project demonstrates to build a robust and scalable data pipeline that can generate, store, process, and analyze data streams in real-time. By leveraging the capabilities of AWS, Snowflake, and Apache NiFi, this pipeline aims to automate data flows, ensure data integrity, and provide immediate analytical insights.

![image](https://github.com/SHREYAS-SHETTY-KR/-Real-Time-Data-Analysis-with-AWS-Snowflake-and-NiFi/assets/79562771/5d145ad5-c577-4214-9c0d-5cb2aced9544)


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

## Visual representations of the project:

<table>
  <tr>
    <td><img src="https://github.com/SHREYAS-SHETTY-KR/-Real-Time-Data-Analysis-with-AWS-Snowflake-and-NiFi/assets/79562771/3830766d-ccb7-4215-8c35-328deb696f78" alt="Screenshot 1" width="300"/></td>
    <td><img src="https://github.com/SHREYAS-SHETTY-KR/-Real-Time-Data-Analysis-with-AWS-Snowflake-and-NiFi/assets/79562771/1940ea98-c064-44a1-b599-7abe46e4badf" alt="Screenshot 2" width="300"/></td>
    <td><img src="https://github.com/SHREYAS-SHETTY-KR/-Real-Time-Data-Analysis-with-AWS-Snowflake-and-NiFi/assets/79562771/49d349d2-c137-41ba-bb69-434b272cb79e" alt="Screenshot 5" width="300"/></td>
  </tr>
  <tr>
    <td><img src="https://github.com/SHREYAS-SHETTY-KR/-Real-Time-Data-Analysis-with-AWS-Snowflake-and-NiFi/assets/79562771/df523c33-0b70-4649-b1b8-24317a736d36" alt="Screenshot 3" width="300"/></td>
    <td><img src="https://github.com/SHREYAS-SHETTY-KR/-Real-Time-Data-Analysis-with-AWS-Snowflake-and-NiFi/assets/79562771/90d71b41-0e3b-4a9b-915a-a54b07744bde" alt="Screenshot 4" width="300"/></td>
    <td><video width="300" controls><source src="URL_TO_YOUR_VIDEO_FILE" type="video/mp4">Your browser does not support the video tag.</video></td>
  </tr>
</table>


## Project Execution Flow

### EC2 Setup

1. Connect to EC2 instance using SSH.
2. Copy project files to EC2 instance.

### Docker Installation

1. Update the software.
2. Install Docker on the EC2 instance.
3. Install Docker Compose.
4. Grant execution permission to Docker Compose.
5. Install pip.
6. Install Docker Compose pip package.

### Docker Configuration

1. Create your `docker-compose.yml` file and store it in a folder with the `.pem` file.
2. Copy files to EC2.
3. Check Docker installation.
4. Start Docker.
5. Verify Docker container is running.

### Running Docker Compose

1. Navigate to the docker-compose directory.
2. Pull images and start services.

### Security Group Configuration

1. Update inbound rules for EC2 instance.
2. Add Custom TCP rules for specific port numbers defined in `docker-compose.yml`.

### Accessing Services

1. Access Jupyter Notebook via EC2 public DNS and port 8888.
2. Access Apache NiFi via EC2 public DNS and port 8080.

### Data Generation with Jupyter

1. Generate fake data using Faker module in Jupyter Notebook.
2. Store fake data in CSV format.
      
### NiFi Configuration

1. Connect to NiFi container.
2. Navigate to the generated fake data file.
3. Configure the NiFi job:
    - Create a processor group with ListFile, FetchFile, and PutS3Object processors.
    - Configure the processors with appropriate settings.

### Snowflake Setup

1. Create tables for customer data.
2. Create a stream on the customer table.
3. Set up stage on the S3 bucket.
4. Create Snowpipe on the stage to copy files into `customer_raw` table.

### Snowpipe and AWS SQS Integration

1. Configure Snowpipe notification channel.
2. Create an event notification in AWS with necessary details.

### Handling Data Changes

1. Create a procedure for incremental data.
2. Use MERGE statements for upsert operations.
3. Automate the procedure with Tasks.
4. Create roles and provide necessary permissions.
5. Schedule tasks to run the procedure at specified intervals.

### Data Analysis

1. Analyze the data stored in Snowflake for real-time insights.
2. Utilize Snowflake features for data querying, manipulation, and visualization.


- Follow these steps to set up and run a real-time data analysis pipeline on an EC2 instance using Docker, Jupyter Notebook, Apache NiFi, and Snowflake.


## Project Structure

```
real-time-data-analysis-pipeline/
├── data_generation/
│   ├── generate_data.ipynb     # Jupyter notebook for generating fake data
├── docker/
│   └── docker-compose.yml      # Docker Compose configuration file
├── nifi/
│   └── nifi_flow.xml           # NiFi flow configuration file (if applicable)
├── scripts/
│   ├── install_docker.sh       # Shell script for Docker installation
│   ├── setup_ec2.sh            # Shell script for EC2 setup
│   └── configure_nifi.sh       # Shell script for NiFi configuration
├── snowflake/
│   ├── create_tables.sql       # SQL script for creating Snowflake tables
│   ├── create_stream.sql       # SQL script for creating Snowflake stream
│   ├── create_stage.sql        # SQL script for creating Snowflake stage
│   └── create_snowpipe.sql     # SQL script for creating Snowpipe
├── .gitignore                  # Git ignore file
├── README.md                   # Project README file
└── requirements.txt            # Python dependencies file
```

## Conclusion
This project demonstrates the end-to-end process of real-time data analysis using AWS, Snowflake, and NiFi. By integrating these technologies, organizations can efficiently manage and analyze large volumes of data, enabling informed decision-making and insights generation. The automation capabilities provided by Snowflake tasks and NiFi streamline the data processing pipeline, enhancing efficiency and scalability.
