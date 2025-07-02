import pika
import pandas as pd
import json

# Koneksi ke RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='query_queue')

# Fungsi callback: eksekusi task
def callback(ch, method, properties, body):
    request = json.loads(body)
    print(f"Dapat request query: {request}")

    # Load data CSV preprocess
    df = pd.read_csv("us_states_preprocessed.csv", parse_dates=["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    # Pivot table (Cube-like)
    pivot = df.pivot_table(
        values=["daily_cases", "daily_deaths", "cases", "deaths"],
        index=["year", "month", "state"],
        aggfunc="sum"
    )
    pivot["case_fatality_rate (%)"] = (pivot["deaths"] / pivot["cases"]) * 100
    pivot = pivot.round(2)

    # Contoh: kirim jawaban 1 (negara dengan total kasus tertinggi)
    if request["query"] == "top_total_cases":
        top_cases = df.groupby("state")["cases"].max().sort_values(ascending=False).head(5)
        result = top_cases.to_dict()

    # Contoh: kirim jawaban 2 (fatality rate tertinggi)
    elif request["query"] == "top_fatality_rate":
        fatality_rate = pivot.groupby("state")["case_fatality_rate (%)"].mean().sort_values(ascending=False).head(5)
        result = fatality_rate.to_dict()

    else:
        result = {"error": "Query tidak dikenali!"}

    # Kirim hasil balik
    response = json.dumps(result)
    ch.basic_publish(exchange='',
                      routing_key=properties.reply_to,
                      properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                      body=response)

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='query_queue', on_message_callback=callback)

print("Worker siap! Tunggu permintaan query...")
channel.start_consuming()
