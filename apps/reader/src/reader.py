import pika
import sys
import json
import os
import time
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


FILE_BUFFERS = {}
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

def receive_and_save_file(queue_name='queue'):
    """
    Connects to RabbitMQ and MinIO.
    """
    rabbitmq_host = os.environ.get('RABBITMQ_URL', 'variable does not exist')
    tasks_queue = 'queue'

    s3_endpoint = os.environ.get('S3_ENDPOINT', 'variable does not exist')
    s3_access_key = os.environ.get('S3_ACCESS_KEY', 'variable does not exist')
    s3_secret_key = os.environ.get('S3_SECRET_KEY', 'variable does not exist')
    s3_region = os.environ.get('S3_REGION', None)
    output_bucket = 'haorui-output-files'

    # MinIO connection
    try:
        minio_client = Minio(
            s3_endpoint,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            region=s3_region,
            secure=True 
        )
        if not minio_client.bucket_exists(output_bucket):
            minio_client.make_bucket(output_bucket)
            print(f"[!] S3 bucket {output_bucket} does not exist, please create it first.", file=sys.stderr)
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
        channel = connection.channel()
        channel.queue_declare(queue=tasks_queue, durable=True)

        def callback(ch, method, properties, body):
            try:
                payload = json.loads(body.decode())
                file_name = payload['file_name']
                line = payload['line_content']
                is_last = payload['is_last_line']

                print(f" [x] Received line for '{file_name}': '{line[:30]}...'")

                if file_name not in FILE_BUFFERS:
                    FILE_BUFFERS[file_name] = []
                FILE_BUFFERS[file_name].append(line)

                # If it's the last line, assemble and upload the file
                if is_last:
                    print(f" [*] Last line received for '{file_name}'. Assembling and uploading...")
                    
                    full_content = "\n".join(FILE_BUFFERS[file_name])
                    content_bytes = full_content.encode('utf-8')
                    content_stream = BytesIO(content_bytes)
                    
                    # Upload to MinIO
                    minio_client.put_object(
                        output_bucket,
                        file_name, # Use the same file name for the output object
                        content_stream,
                        len(content_bytes),
                        content_type='text/plain'
                    )
                    
                    print(f" [✔] Successfully uploaded '{file_name}' to bucket '{output_bucket}'.")
                    
                    # Clean up the buffer for this file
                    del FILE_BUFFERS[file_name]

            except json.JSONDecodeError:
                print("[!] Received a malformed message.", file=sys.stderr)
            except S3Error as e:
                print(f"[!] Error uploading to MinIO: {e}", file=sys.stderr)
            except Exception as e:
                print(f"[!] An error occurred in the callback: {e}", file=sys.stderr)
            finally:
                # Acknowledge the message
                ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"[!] Error: Could not connect to RabbitMQ. Please ensure it is running.", file=sys.stderr)
    except KeyboardInterrupt:
        print("\n[*] Interrupted by user. Shutting down.")
    except Exception as e:
        print(f"[!] An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()
            print("[*] Connection closed.")

if __name__ == '__main__':
    health_thread = threading.Thread(target=run_health_check_server, daemon=True)
    health_thread.start()

    receive_and_save_file()
