import json
import boto3
import fastparquet
import psycopg2
from io import BytesIO
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

        # Read Parquet file into a DataFrame using fastparquet
        parquet_file = BytesIO(body.read())
        parquet_data = fastparquet.ParquetFile(parquet_file)
        
        # Convert Parquet data to a pandas DataFrame
        df = parquet_data.to_pandas()

        # Define the new column names mapping
        new_column_names = {
            'index': 'index',
            'engine': 'engine',
            'cycles': 'cycles',
            'alt': 'altitude (alt)',
            'mach': 'mach_no (mach)',
            'TRA': 'throttle_angle (TRA)',
            'T2': 'fan_inlet_temp (T2)',
            'T24': 'LPC_outlet_temp (T24)',
            'T30': 'HPC_outlet_temp (T30)',
            'T50': 'LPT_outlet_temp (T50)',
            'P2': 'fan_inlet_pressure (P2)',
            'P15': 'bypass_duct_pressure (P15)',
            'P30': 'HPC_outlet_pressure (P30)',
            'Nf': 'fan_speed (Nf)',
            'Nc': 'core_speed (Nc)',
            'epr': 'engine_pressure_ratio (epr)',
            'Ps30': 'HPC_outlet_static_pressure (Ps30)',
            'phi': 'fuel_ps30_ratio (phi)',
            'NRf': 'corrected_fan_speed (NRf)',
            'NRc': 'corrected_core_speed (NRc)',
            'BPR': 'bypass_ratio (BPR)',
            'farB': 'burner_fuel_air_ratio (farB)',
            'htBleed': 'bleed_enthalpy (htBleed)',
            'Nf_dmd': 'demanded_fan_speed (Nf_dmd)',
            'PCNfR_dmd': 'demanded_corrected_fan_speed (PCNfR_dmd)',
            'W31': 'HPT_coolant_bleed (W31)',
            'W32': 'LPT_coolant_bleed (W32)',
            'source': 'source'
        }

        # Rename the columns
        df.rename(columns=new_column_names, inplace=True)

        # Define the schema with new column names and data types
        schema = {
            'index': 'INTEGER',
            'engine': 'INTEGER',
            'cycles': 'INTEGER',
            '"altitude (alt)"': 'FLOAT',
            '"mach_no (mach)"': 'FLOAT',
            '"throttle_angle (TRA)"': 'FLOAT',
            '"fan_inlet_temp (T2)"': 'FLOAT',
            '"LPC_outlet_temp (T24)"': 'FLOAT',
            '"HPC_outlet_temp (T30)"': 'FLOAT',
            '"LPT_outlet_temp (T50)"': 'FLOAT',
            '"fan_inlet_pressure (P2)"': 'FLOAT',
            '"bypass_duct_pressure (P15)"': 'FLOAT',
            '"HPC_outlet_pressure (P30)"': 'FLOAT',
            '"fan_speed (Nf)"': 'FLOAT',
            '"core_speed (Nc)"': 'FLOAT',
            '"engine_pressure_ratio (epr)"': 'FLOAT',
            '"HPC_outlet_static_pressure (Ps30)"': 'FLOAT',
            '"fuel_ps30_ratio (phi)"': 'FLOAT',
            '"corrected_fan_speed (NRf)"': 'FLOAT',
            '"corrected_core_speed (NRc)"': 'FLOAT',
            '"bypass_ratio (BPR)"': 'FLOAT',
            '"burner_fuel_air_ratio (farB)"': 'FLOAT',
            '"bleed_enthalpy (htBleed)"': 'INTEGER',
            '"demanded_fan_speed (Nf_dmd)"': 'INTEGER',
            '"demanded_corrected_fan_speed (PCNfR_dmd)"': 'FLOAT',
            '"HPT_coolant_bleed (W31)"': 'FLOAT',
            '"LPT_coolant_bleed (W32)"': 'FLOAT',
            '"source"': 'INTEGER'
        }

        columns_to_round_2 = [
            'fan_inlet_temp (T2)', 'LPC_outlet_temp (T24)', 'HPC_outlet_temp (T30)', 'LPT_outlet_temp (T50)', 
            'fan_inlet_pressure (P2)', 'bypass_duct_pressure (P15)', 'HPC_outlet_pressure (P30)', 
            'fan_speed (Nf)', 'core_speed (Nc)', 'engine_pressure_ratio (epr)', 'HPC_outlet_static_pressure (Ps30)', 
            'fuel_ps30_ratio (phi)', 'corrected_fan_speed (NRf)', 'corrected_core_speed (NRc)', 
            'burner_fuel_air_ratio (farB)', 'demanded_corrected_fan_speed (PCNfR_dmd)', 'HPT_coolant_bleed (W31)'
        ]
        
        columns_to_round_4 = [
            'altitude (alt)', 'mach_no (mach)', 'bypass_ratio (BPR)', 'LPT_coolant_bleed (W32)'
        ]

        df[columns_to_round_2] = df[columns_to_round_2].round(2)
        df[columns_to_round_4] = df[columns_to_round_4].round(4)

        # Convert DataFrame rows to list of tuples for insertion
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Escape column names with spaces or special characters
        escaped_columns = [f'"{column}"' if ' ' in column or '(' in column or ')' in column else column for column in df.columns]

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
