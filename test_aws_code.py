import boto3
import psycopg2

def fetch_data_from_s3(file_location):
    # Replace with your AWS credentials and S3 bucket details
    aws_access_key_id = 'your-access-key-id'
    aws_secret_access_key = 'your-secret-access-key'
    s3_bucket = 'your-s3-bucket-name'

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=file_location)
        data = response['Body'].read().decode('utf-8')  # Assuming the data is UTF-8 encoded

        return data
    except Exception as e:
        print(f"Error fetching data from S3: {e}")
        raise e

def lambda_handler(event, context):
    # Replace these values with your source RDS credentials and connection details
    source_rds_host = "your-source-rds-hostname.amazonaws.com"
    source_db_name = "your-source-database-name"
    source_username = "your-source-username"
    source_password = "your-source-password"
    source_port = 5432  # Change this to your source database port

    # Replace these values with your destination RDS credentials and connection details
    dest_rds_host = "your-destination-rds-hostname.amazonaws.com"
    dest_db_name = "your-destination-database-name"
    dest_username = "your-destination-username"
    dest_password = "your-destination-password"
    dest_port = 5432  # Change this to your destination database port

    # Connect to the source RDS database
    try:
        source_conn = psycopg2.connect(
            host=source_rds_host,
            user=source_username,
            password=source_password,
            database=source_db_name,
            port=source_port
        )
    except Exception as e:
        print(f"Error connecting to the source database: {e}")
        raise e

    # Connect to the destination RDS database
    try:
        dest_conn = psycopg2.connect(
            host=dest_rds_host,
            user=dest_username,
            password=dest_password,
            database=dest_db_name,
            port=dest_port
        )
    except Exception as e:
        print(f"Error connecting to the destination database: {e}")
        raise e

    # Query the source database
    source_query = "SELECT file_location, metadata, type FROM your_source_table;"
    try:
        with source_conn.cursor() as source_cursor:
            source_cursor.execute(source_query)
            result = source_cursor.fetchall()
            print("Query result:", result)
    except Exception as e:
        print(f"Error executing the source query: {e}")
        raise e
    finally:
        # Close the source database connection
        source_conn.close()

    # Fetch data from S3 based on file_location
    for row in result:
        file_location, metadata, file_type = row
        s3_data = fetch_data_from_s3(file_location)

        # Do something with the fetched data, for example, insert it into the destination database
        # Modify this part according to your specific use case

        # Insert into the destination database
        dest_table = "your_destination_table"
        insert_query = "INSERT INTO {} (file_location, metadata, type, s3_data) VALUES (%s, %s, %s, %s);".format(dest_table)
        try:
            with dest_conn.cursor() as dest_cursor:
                dest_cursor.execute(insert_query, (file_location, metadata, file_type, s3_data))
                dest_conn.commit()
                print("Data inserted into destination table.")
        except Exception as e:
            print(f"Error inserting data into the destination table: {e}")
            raise e

    # Close the destination database connection
    dest_conn.close()

    # Return a response indicating success
    return {
        'statusCode': 200,
        'body': 'Data transferred successfully!'
    }