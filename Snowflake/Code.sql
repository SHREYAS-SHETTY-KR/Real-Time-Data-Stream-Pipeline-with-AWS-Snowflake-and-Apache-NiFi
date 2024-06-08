-- Switch to the specified database and schema
USE DATABASE SCD_DB;
USE SCHEMA SCD_SC;

-- Create the CUSTOMER table to store current customer data
CREATE OR REPLACE TABLE CUSTOMER (
     CUSTOMER_ID NUMBER,
     FIRST_NAME VARCHAR,
     LAST_NAME VARCHAR,
     EMAIL VARCHAR,
     STREET VARCHAR,
     CITY VARCHAR,
     STATE VARCHAR,
     COUNTRY VARCHAR,
     UPDATE_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create the CUSTOMER_HISTORY table to store historical customer data
CREATE OR REPLACE TABLE CUSTOMER_HISTORY (
     CUSTOMER_ID NUMBER,
     FIRST_NAME VARCHAR,
     LAST_NAME VARCHAR,
     EMAIL VARCHAR,
     STREET VARCHAR,
     CITY VARCHAR,
     STATE VARCHAR,
     COUNTRY VARCHAR,
     START_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
     END_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
     IS_CURRENT BOOLEAN
);

-- Create the CUSTOMER_RAW table to store raw customer data
CREATE OR REPLACE TABLE CUSTOMER_RAW (
     CUSTOMER_ID NUMBER,
     FIRST_NAME VARCHAR,
     LAST_NAME VARCHAR,
     EMAIL VARCHAR,
     STREET VARCHAR,
     CITY VARCHAR,
     STATE VARCHAR,
     COUNTRY VARCHAR
);

-- Show all tables in the current schema
SHOW TABLES;

-- Create a stream to capture changes in the CUSTOMER table
CREATE OR REPLACE STREAM CUSTOMER_TABLE_CHANGES ON TABLE CUSTOMER;

-- Create a storage integration for connecting to S3
CREATE OR REPLACE STORAGE INTEGRATION S3_CONN
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::837934442683:role/snowflake_s3_conn'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-project-bucket-shrey')
COMMENT = 'Creating connection to s3';

-- Describe the storage integration
DESC INTEGRATION S3_CONN;

-- Create a file format for CSV files
CREATE OR REPLACE FILE FORMAT SCD_DB.SCD_SC.CSV_FILE_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE;

-- Create an external stage for loading data from S3
CREATE OR REPLACE STAGE SCD_DB.SCD_SC.CUSTOMER_EXT_STAGE
URL = 's3://snowflake-project-bucket-shrey/Stream_data/'
STORAGE_INTEGRATION = S3_CONN
FILE_FORMAT = SCD_DB.SCD_SC.CSV_FILE_FORMAT;

-- List files in the external stage
LIST @SCD_DB.SCD_SC.CUSTOMER_EXT_STAGE;

-- Create a pipe to load data from the external stage into the CUSTOMER_RAW table
CREATE OR REPLACE PIPE CUSTOMER_RAW_S3_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO CUSTOMER_RAW
FROM @SCD_DB.SCD_SC.CUSTOMER_EXT_STAGE
FILE_FORMAT = SCD_DB.SCD_SC.CSV_FILE_FORMAT;

-- Show all pipes
SHOW PIPES;

-- Check the status of the pipe
SELECT SYSTEM$PIPE_STATUS('CUSTOMER_RAW_S3_PIPE');

-- Count the number of rows in the CUSTOMER_RAW table
SELECT COUNT(*) FROM CUSTOMER_RAW;

-- Merge data from the CUSTOMER_RAW table into the CUSTOMER table
MERGE INTO CUSTOMER C
USING CUSTOMER_RAW CR
ON C.CUSTOMER_ID = CR.CUSTOMER_ID
WHEN MATCHED AND (C.CUSTOMER_ID <> CR.CUSTOMER_ID OR
                   C.FIRST_NAME  <> CR.FIRST_NAME  OR
                   C.LAST_NAME   <> CR.LAST_NAME   OR
                   C.EMAIL       <> CR.EMAIL       OR
                   C.STREET      <> CR.STREET      OR
                   C.CITY        <> CR.CITY        OR
                   C.STATE       <> CR.STATE       OR
                   C.COUNTRY     <> CR.COUNTRY)    THEN UPDATE
    SET  C.CUSTOMER_ID = CR.CUSTOMER_ID,
         C.FIRST_NAME  = CR.FIRST_NAME,  
         C.LAST_NAME   = CR.LAST_NAME,   
         C.EMAIL       = CR.EMAIL,       
         C.STREET      = CR.STREET,      
         C.CITY        = CR.CITY,       
         C.STATE       = CR.STATE,
         C.COUNTRY     = CR.COUNTRY,
         UPDATE_TIMESTAMP = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (C.CUSTOMER_ID, C.FIRST_NAME, C.LAST_NAME, C.EMAIL, C.STREET, C.CITY, C.STATE, C.COUNTRY)
    VALUES (CR.CUSTOMER_ID, CR.FIRST_NAME, CR.LAST_NAME, CR.EMAIL, CR.STREET, CR.CITY, CR.STATE, CR.COUNTRY);

-- Show the contents of the CUSTOMER table
SELECT * FROM CUSTOMER;

-- Create a JavaScript procedure for performing SCD operations
CREATE OR REPLACE PROCEDURE PDR_SCD()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
   $$
     var cmd = `MERGE INTO CUSTOMER C
                       USING CUSTOMER_RAW CR
                       ON C.CUSTOMER_ID = CR.CUSTOMER_ID
                       WHEN MATCHED AND ( C.CUSTOMER_ID <> CR.CUSTOMER_ID OR
                                          C.FIRST_NAME  <> CR.FIRST_NAME  OR
                                          C.LAST_NAME   <> CR.LAST_NAME   OR
                                          C.EMAIL       <> CR.EMAIL       OR
                                          C.STREET      <> CR.STREET      OR
                                          C.CITY        <> CR.CITY        OR
                                          C.STATE       <> CR.STATE       OR
                                          C.COUNTRY     <> CR.COUNTRY)    THEN UPDATE
                           SET  C.CUSTOMER_ID = CR.CUSTOMER_ID,
                                C.FIRST_NAME  = CR.FIRST_NAME,  
                                C.LAST_NAME   = CR.LAST_NAME,   
                                C.EMAIL       = CR.EMAIL,       
                                C.STREET      = CR.STREET,      
                                C.CITY        = CR.CITY,       
                                C.STATE       = CR.STATE,
                                C.COUNTRY     = CR.COUNTRY,
                                UPDATE_TIMESTAMP = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
    INSERT (C.CUSTOMER_ID, C.FIRST_NAME, C.LAST_NAME, C.EMAIL, C.STREET, C.CITY, C.STATE, C.COUNTRY)
    VALUES (CR.CUSTOMER_ID, CR.FIRST_NAME, CR.LAST_NAME, CR.EMAIL, CR.STREET, CR.CITY, CR.STATE, CR.COUNTRY);`;

    var cmd1 = "TRUNCATE TABLE SCD_DB.SCD_SC.CUSTOMER_RAW;"
      var sql = snowflake.createStatement({sqlText: cmd});
      var sql1 = snowflake.createStatement({sqlText: cmd1});
      var result = sql.execute();
      var result1 = sql1.execute();
    return cmd + '\n' + cmd1;
    $$;

-- Call the SCD procedure to perform SCD operations
CALL PDR_SCD();

-- Create a new role for managing tasks
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE TASKADMIN;

-- Grant necessary privileges to the TASKADMIN role
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TASKADMIN;
USE ROLE SECURITYADMIN;
GRANT ROLE TASKADMIN TO ROLE SYSADMIN;

-- Create a task for executing the SCD procedure at regular intervals
CREATE OR REPLACE TASK TSK_SCD_RAW WAREHOUSE = COMPUTE_WH SCHEDULE = '1 MINUTE'
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
CALL PDR_SCD();

-- Show all tasks
SHOW TASKS;

-- Suspend the task (optional)
ALTER TASK TSK_SCD_RAW SUSPEND;

-- Show the next scheduled run time of the task
SELECT TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) AS NEXT_RUN, SCHEDULED_TIME, CURRENT_TIMESTAMP, NAME, STATE 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
WHERE STATE = 'SCHEDULED' 
ORDER BY COMPLETED_TIME DESC;

-- Show the contents of the CUSTOMER_RAW table
SELECT * FROM CUSTOMER_RAW;

-- Show the contents of the CUSTOMER table
SELECT * FROM CUSTOMER;

-- Show the contents of the CUSTOMER_HISTORY table
SELECT * FROM CUSTOMER_HISTORY;

-- Create a view to display change data capture (CDC) information
CREATE OR REPLACE VIEW V_CUSTOMER_CHANGE_DATA AS
-- Subquery for INSERT operations
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, START_TIME, END_TIME, IS_CURRENT,
'I' AS DML_TYPE
FROM
    (SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, UPDATE_TIMESTAMP AS START_TIME,
    LAG(UPDATE_TIMESTAMP) OVER (PARTITION BY CUSTOMER_ID ORDER BY UPDATE_TIMESTAMP DESC) AS END_TIME_RAW,
    CASE WHEN END_TIME_RAW IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE END_TIME_RAW END AS END_TIME,
    CASE WHEN END_TIME_RAW IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT
    FROM
        (SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, UPDATE_TIMESTAMP
        FROM SCD_DB.SCD_SC.CUSTOMER_TABLE_CHANGES
        WHERE metadata$action = 'INSERT'AND metadata$isupdate = 'FALSE'))
UNION
-- Subquery for UPDATE and DELETE operations
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, START_TIME, END_TIME, IS_CURRENT, DML_TYPE
FROM
(SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, UPDATE_TIMESTAMP AS START_TIME,
LAG(UPDATE_TIMESTAMP) OVER (PARTITION BY CUSTOMER_ID ORDER BY UPDATE_TIMESTAMP DESC) AS END_TIME_RAW,
CASE WHEN END_TIME_RAW IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE END_TIME_RAW END AS END_TIME,
CASE WHEN END_TIME_RAW IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT, DML_TYPE
FROM(
        -- Identify data to insert into customer_history table
        SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, UPDATE_TIMESTAMP, 'I' AS DML_TYPE
        FROM CUSTOMER_TABLE_CHANGES
        WHERE metadata$action = 'INSERT'
        AND metadata$isupdate = 'TRUE'
        UNION
        -- Identify data in customer_HISTORY table that needs to be updated
        SELECT CUSTOMER_ID, NULL, NULL, NULL, NULL, NULL, NULL, NULL, START_TIME, 'U' AS DML_TYPE
        FROM CUSTOMER_HISTORY
        WHERE CUSTOMER_ID IN
            (SELECT DISTINCT CUSTOMER_ID
            FROM CUSTOMER_TABLE_CHANGES
            WHERE metadata$action = 'DELETE'
            AND metadata$isupdate = 'TRUE')
        AND IS_CURRENT = TRUE))
UNION
-- Subquery for DELETE operations
SELECT CTC.CUSTOMER_ID, NULL, NULL, NULL, NULL, NULL, NULL, NULL, CH.START_TIME, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
NULL, 'D' AS DML_TYPE
FROM
CUSTOMER_HISTORY CH
INNER JOIN CUSTOMER_TABLE_CHANGES CTC
ON CH.CUSTOMER_ID = CTC.CUSTOMER_ID
WHERE CTC.metadata$action = 'DELETE'
AND CTC.metadata$isupdate = 'FALSE'
AND CH.IS_CURRENT = TRUE;

-- Show all tasks
SHOW TASKS;

-- Suspend the task (optional)
ALTER TASK TSK_SCD_HIST SUSPEND;

-- Insert, update, or delete data in the CUSTOMER table (for testing purposes)
INSERT INTO CUSTOMER VALUES(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut','Cape Verde',CURRENT_TIMESTAMP());

UPDATE CUSTOMER SET FIRST_NAME='Jessica' WHERE CUSTOMER_ID=7523;

DELETE FROM CUSTOMER WHERE CUSTOMER_ID =136 AND FIRST_NAME = 'Kim';

-- Check the count of unique customer IDs in the CUSTOMER table
SELECT COUNT(*),CUSTOMER_ID FROM CUSTOMER GROUP BY CUSTOMER_ID HAVING COUNT(*)=1;

-- Show the change history of a specific customer
SELECT * FROM CUSTOMER_HISTORY WHERE CUSTOMER_ID =7523;

-- Show the current data in the CUSTOMER_HISTORY table
SELECT * FROM CUSTOMER_HISTORY WHERE IS_CURRENT=TRUE;

-- Show the scheduled tasks
SELECT TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) AS NEXT_RUN, SCHEDULED_TIME, CURRENT_TIMESTAMP, NAME, STATE 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE STATE = 'SCHEDULED' ORDER BY COMPLETED_TIME DESC;

-- Show historical data in the CUSTOMER_HISTORY table
SELECT * FROM CUSTOMER_HISTORY WHERE IS_CURRENT=FALSE;

-- Show all tasks
SHOW TASKS;

-- Show the contents of the CUSTOMER table (SCD1 table)
SELECT * FROM CUSTOMER;

-- Show the contents of the CUSTOMER_RAW table (staging table)
SELECT * FROM CUSTOMER_RAW;

-- Show the contents of the CUSTOMER_HISTORY table (SCD2 table)
SELECT * FROM CUSTOMER_HISTORY;
