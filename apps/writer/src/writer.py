import pika
import os
import time
import sys
from minio import Minio
from minio.error import S3Error
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

HEALTH_CHECK_PORT = 8080
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/healthz':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'ok')
        else:
            self.send_response(404)
            self.end_headers()

def run_health_check_server():
    try:
        server_address = ('0.0.0.0', HEALTH_CHECK_PORT)
        httpd = HTTPServer(server_address, HealthCheckHandler)
        httpd.serve_forever()
    except Exception as e:
        print(f"[!] Health check server failed: {e}", file=sys.stderr)

def process_and_send_files():
    """
    Connects to RabbitMQ and MinIO.
    """
    rabbitmq_host = os.environ.get('RABBITMQ_URL', 'variable does not exist')
    files_storage = 'files' 
    tasks_exchange = 'queue'

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
        # Channel for event-trigger by the Bucket
        read_bucket_channel = connection.channel()
        read_bucket_channel.queue_declare(queue=files_storage, durable=True)

        # Channel for write the file
        write_channel = connection.channel()
        write_channel.exchange_declare(
            exchange=tasks_exchange,
            exchange_type='x-consistent-hash',
            durable=True
        )

        def callback(ch, method, properties, body):
            file_name = body.decode()
            print(f" [x] Received job for file: '{file_name}'")

            try:
                # Download file from MinIO 
                response = minio_client.get_object(s3_bucket, file_name)
                print(f" [*] Successfully downloaded '{file_name}' from MinIO.")
                
                # Read file and publish by lines
                file_content = response.read().decode('utf-8')
                lines = file_content.splitlines()
                num_lines = len(lines)
                
                for i, line in enumerate(lines):
                    is_last = (i == num_lines - 1)
                    
                    message_payload = {
                        'file_name': file_name,
                        'line_content': line,
                        'is_last_line': is_last
                    }
                    
                    message_body = json.dumps(message_payload)
                    
                    write_channel.basic_publish(
                        exchange=tasks_exchange,
                        routing_key=file_name, 
                        body=message_body,
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                print(f" [✔] Finished processing '{file_name}'.")
                

            except S3Error as e:
                print(f" [!] Error accessing file '{file_name}' in MinIO: {e}", file=sys.stderr)
            finally:
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
    
    health_thread = threading.Thread(target=run_health_check_server, daemon=True)
    health_thread.start()

    process_and_send_files()
