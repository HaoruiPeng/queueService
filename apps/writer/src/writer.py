import pika
import os
import time
import sys
from minio import Minio
from minio.error import S3Error

def process_and_send_files():
    """
    Connects to RabbitMQ and MinIO.
    """
    rabbitmq_host = os.environ.get('RABBITMQ_URL', 'variable does not exist')
    files_storage = 'files' 
    tasks_queue = 'queue'

    s3_endpoint = os.environ.get('S3_ENDPOINT', 'variable does not exist')
    s3_access_key = os.environ.get('S3_ACCESS_KEY', 'variable does not exist')
    s3_secret_key = os.environ.get('S3_SECRET_KEY', 'variable does not exist')
    s3_region = os.environ.get('S3_REGION', None)
    s3_bucket = 'haorui-files-storage'

    # MinIO connection
    try:
        minio_client = Minio(
            s3_endpoint,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            region=s3_region,
            secure=True
        )
        # Ensure the bucket exists
        if not minio_client.bucket_exists(s3_bucket):
            minio_client.make_bucket(s3_bucket)
            print(f"[!] S3 bucket {s3_bucket} does not exist, please create it first.", file=sys.stderr)
            sys.exit(1)
    except S3Error as e:
        print(f"[!] Error connecting to MinIO: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[!] An unexpected error occurred with MinIO setup: {e}", file=sys.stderr)
        sys.exit(1)


    #  RabbitMQ Connection
    connection = None
    while not connection:
        try:
            print(f"[*] Attempting to connect to RabbitMQ at '{rabbitmq_host}'...")
            params = pika.URLParameters(rabbitmq_host)
            connection = pika.BlockingConnection(params)
            print("[*] Successfully connected to RabbitMQ.")
        except pika.exceptions.AMQPConnectionError:
            print("[!] RabbitMQ is not available. Retrying in 5 seconds...", file=sys.stderr)
            time.sleep(5)

    try:
        # Establish two channels for reading and writing
        read_bucket_channel = connection.channel()
        read_bucket_channel.queue_declare(queue=files_storage, durable=True)

        write_channel = connection.channel()
        write_channel.queue_declare(queue=tasks_queue, durable=True)

        def callback(ch, method, properties, body):
            """This function is called when a message is received."""
            file_name = body.decode()
            print(f" [x] Received job for file: '{file_name}'")

            try:
                # --- Download file from MinIO ---
                response = minio_client.get_object(s3_bucket, file_name)
                print(f" [*] Successfully downloaded '{file_name}' from MinIO.")
                
                # --- Read file and publish lines ---
                file_content = response.read().decode('utf-8')
                lines = file_content.splitlines()
                
                for line in lines:
                    if line: # Ensure we don't send empty lines
                        write_channel.basic_publish(
                            exchange='',
                            routing_key=tasks_queue,
                            body=line,
                            properties=pika.BasicProperties(delivery_mode=2)
                        )
                        print(f"   [>] Sent line: '{line[:30]}...'")
                
                print(f" [✔] Finished processing '{file_name}'.")

            except S3Error as e:
                print(f" [!] Error accessing file '{file_name}' in MinIO: {e}", file=sys.stderr)
            finally:
                # Acknowledge the message was processed (or failed)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if 'response' in locals():
                    response.close()
                    response.release_conn()

        read_bucket_channel.basic_qos(prefetch_count=1)
        read_bucket_channel.basic_consume(queue=files_storage, on_message_callback=callback)

        print('[*] Waiting for file processing jobs. To exit press CTRL+C')
        read_bucket_channel.start_consuming()

    except KeyboardInterrupt:
        print('\n[*] Shutting down writer.')
    except Exception as e:
        print(f"[!] An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if connection and connection.is_open:
            connection.close()
            print("[*] Connection closed.")

if __name__ == '__main__':
    process_and_send_files()
