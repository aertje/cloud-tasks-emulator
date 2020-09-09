from google.auth.credentials import AnonymousCredentials
from google.cloud import tasks_v2
import grpc

grpc_channel = grpc.insecure_channel('localhost:8123')

client = tasks_v2.CloudTasksClient(channel=grpc_channel)

parent = client.location_path('aert-sandbox', 'us-central1')

queue_name = parent + '/queues/test'

client.create_queue(parent, {'name': queue_name})


client.create_task(queue_name, {'http_request': {'http_method': 'POST', 'url': 'https://www.google.com/vf'}}) # 404s
