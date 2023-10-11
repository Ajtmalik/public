
from confluent_kafka import Consumer
import json
import pandas as pd
from sqlalchemy import create_engine


def commit_completed(err, partitions):
	if err:
		print(str(err))
	else:
		print("Committed partition offsets: " + str(partitions))

conf = {'bootstrap.servers': 'broker:29092','group.id': "foo",'default.topic.config': {'auto.offset.reset': 'smallest'},'on_commit': commit_completed}

consumer = Consumer(conf)

running = True

def shutdown():
	running = False

MIN_COMMIT_COUNT=30
lat_op={}
def consume_loop(consumer, topics):
	try:
		consumer.subscribe(topics)
		msg_count = 0
		msg_list=[]
		while running:
			msg = consumer.poll(timeout=1.0)
			if msg is None: continue
			if msg.error():
				if msg.error().code() == KafkaError._PARTITION_EOF:
					# End of partition event
					sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
									 (msg.topic(), msg.partition(), msg.offset()))
				elif msg.error():
					raise KafkaException(msg.error())
			else:
				msg_list.append(json.loads(msg.value()))
				msg_count += 1
				print(msg_count)
				if msg_count % MIN_COMMIT_COUNT == 0:
					consumer.commit(asynchronous=True)
					msg_process(msg_list)
					msg_list=[]
				#shutdown()
	finally:
		# Close down consumer to commit final offsets.
		consumer.close()


def msg_process(msg_list):
	df=pd.DataFrame(msg_list)
	engine = create_engine('postgresql://db_user:pwd@db:5432/mydb')
	df.to_sql('table_test_kafka', engine, if_exists='append')

if __name__ == "__main__":
	consume_loop(consumer,['test'])
