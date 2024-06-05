from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'topic4',
    group_id='test-group', # 컨슈머 그룹 식별자
    bootstrap_servers=['localhost:9092'], # 카프카 브로커 주소 리스트
    auto_offset_reset='latest', # 오프셋 위치(earliest:가장 처음, latest: 가장 최근)
    enable_auto_commit=False, # 오프셋 자동 커밋 여부
    value_deserializer=lambda x: loads(x.decode('utf-8')), # 메시지의 값 역직렬화
    key_deserializer=lambda x: loads(x.decode('utf-8'))
    # consumer_timeout_ms=1000, # 데이터를 기다리는 최대 시간
    # session_timeout_ms=30000,  # 기본값은 10000ms
    # heartbeat_interval_ms=10000,  # 기본값은 3000ms    
)

# consumer.subscribe('topic1')

print('[Start] get consumer')

for message in consumer:    
    print(f'Topic : {message.topic}, Partition : {message.partition}, Offset : {message.offset}, Key : {message.key}, value : {message.value}')

print('[End] get consumer')

# from kafka import KafkaConsumer

# # To consume latest messages and auto-commit offsets
# consumer = KafkaConsumer('my-topic',
#                          group_id='my-group',
#                          bootstrap_servers=['localhost:9092'])
# for message in consumer:
#     # message value and key are raw bytes -- decode if necessary!
#     # e.g., for unicode: `message.value.decode('utf-8')`
#     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                           message.offset, message.key,
#                                           message.value))