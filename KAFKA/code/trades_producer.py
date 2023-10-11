import json
import requests
import time
from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'broker:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

running = True

def produce_loop():
	try:
		while running:
			url = 'https://test.deribit.com/api/v2/public/get_last_trades_by_instrument?count=10&instrument_name=BTC-PERPETUAL'
			headers = {'content-type': 'application/json'}
			r = requests.get(url, headers=headers)
			for data in r.json()['result']['trades']:
				producer.produce('test', json.dumps(data).encode('utf-8'))
			print('One Batch DOne!')
			time.sleep(10)
	finally:
		# Close down consumer to commit final offsets.
		pass

if __name__ == "__main__":
	produce_loop()