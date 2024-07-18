import json
import boto3
import pandas as pd
import psycopg2
from io import StringIO
from psycopg2 import extras
import os

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
sns_client = boto3.client('sns')

def send_to_sqs(message, queue_url):
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )
    return response

def publish_to_sns(message, topic_arn):
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
        Subject='Lambda Function Error Notification'
    )
    return response

def lambda_handler(event, context):
    try:
        # Retrieve bucket and file name from the event
        s3_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]

        # Get the file object from S3
        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_file_name)
        body = s3_object['Body']

        # Read text file into a pandas DataFrame
        text_file = StringIO(body.read().decode('utf-8'))
        df = pd.read_csv(text_file)

        # Define the new column names mapping
        new_column_names = {
            'index': 'index',
            'RUL': 'remaining_useful_life'
        }

        # Rename the columns
        df.rename(columns=new_column_names, inplace=True)

        # Define the schema with new column names and data types
        schema = {
            'index': 'INTEGER',
            'remaining_useful_life': 'INTEGER'
        }

        # Convert DataFrame rows to list of tuples for insertion
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Escape column names with spaces
        escaped_columns = [f'"{column}"' if ' ' in column else column for column in df.columns]

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=os.environ['db_name'],
            user=os.environ['db_user'],
            password=os.environ['db_password'],
            host=os.environ['db_host'],
            port=os.environ['db_port']
        )
        cur = conn.cursor()

        # Define the table name
        table_name = os.environ['table_name']

        # Generate the create table query dynamically based on the schema
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} {dtype}' for col, dtype in schema.items()])})"
        cur.execute(create_table_query)

        # Generate the insert query dynamically based on the columns
        insert_query = f"INSERT INTO {table_name} ({', '.join(escaped_columns)}) VALUES %s"

        # Insert data into the PostgreSQL database
        extras.execute_values(cur, insert_query, rows)
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

    except Exception as err:
        print(err)
        error_message = {
            'error': str(err),
            'event': event
        }
        # Send error details to SQS queue
        send_to_sqs(error_message, os.environ['sqs_queue_url'])
        # Publish error details to SNS topic
        publish_to_sns(error_message, os.environ['sns_topic_arn'])

    # Return a response
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
