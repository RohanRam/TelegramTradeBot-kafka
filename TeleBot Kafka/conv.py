from kafka import KafkaConsumer
from trade_pb2 import TradeAction
 
consumer = KafkaConsumer(
    'telebot',
    bootstrap_servers=['localhost:9092'],
    # auto_offset_reset='earliest'
    enable_auto_commit=True,
    group_id='86'
)
 
print('Consumer is running...')
try:
    for message in consumer:
        
        trade_action = TradeAction()
        trade_action.ParseFromString(message.value)
        print(f'TRADE INFORMATION: Stock={trade_action.stock}, Quantity={trade_action.quantity}')
except KeyboardInterrupt:
    print('Consumer stopped.')
except Exception as e:
    print(f'Error in consumer: {e}')
finally:
    consumer.close()