import pika
import uuid
import json

# Class RPC Client
class QueryRpcClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(queue=self.callback_queue,
                                   on_message_callback=self.on_response,
                                   auto_ack=True)
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, query_type):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        request = json.dumps({"query": query_type})
        self.channel.basic_publish(exchange='',
                                   routing_key='query_queue',
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.corr_id
                                   ),
                                   body=request)
        while self.response is None:
            self.connection.process_data_events()
        return json.loads(self.response)

# Kirim permintaan query
query_rpc = QueryRpcClient()

# Pilih query
print("🔎 Pilih query:")
print("1. top_total_cases")
print("2. top_fatality_rate")
query_choice = input("Masukkan pilihan query (1/2): ")

if query_choice == "1":
    query_type = "top_total_cases"
elif query_choice == "2":
    query_type = "top_fatality_rate"
else:
    print("Query tidak valid.")
    exit()

print(f"Kirim permintaan query: {query_type}")
response = query_rpc.call(query_type)
print("Hasil dari worker:")
print(response)
