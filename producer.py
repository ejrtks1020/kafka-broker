from kafka import KafkaProducer
from json import dumps
import time

producer = KafkaProducer(
    acks=0, # 메시지 전송 완료에 대한 체크
    compression_type='gzip', # 메시지 전달할 때 압축(None, gzip, snappy, lz4 등)
    bootstrap_servers=['localhost:9092'], # 전달하고자 하는 카프카 브로커의 주소 리스트
    value_serializer=lambda x:dumps(x).encode('utf-8'), # 메시지의 값 직렬화
    key_serializer=lambda x:dumps(x).encode('utf-8')
)

start = time.time()

for i in range(10):
    data = {'str' : 'result'+str(i)}
    producer.send('topic4', value=data, key=f"key {i}")

producer.flush() 

print('[Done]:', time.time() - start)

# from kafka import KafkaProducer
# from kafka.errors import KafkaError

# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# # Asynchronous by default
# future = producer.send('my-topic', b'raw_bytes')

# # Block for 'synchronous' sends
# try:
#     record_metadata = future.get(timeout=10)
# except KafkaError:
#     # Decide what to do if produce request failed...
#     print("kafka error")
#     pass

# # Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)