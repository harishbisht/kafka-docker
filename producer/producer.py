from time import sleep
from kafka import KafkaProducer
import random, string, datetime
from protobuf3.message import Message
from protobuf3.fields import StringField, Int32Field, MessageField

producer = KafkaProducer(bootstrap_servers=['kafka-1:19092'])


def __id_generator__(size=6, chars=string.ascii_uppercase + string.digits + string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


def generate_random_1000_transaction_data(data_obj):
    for _ in range(0,1000):
        transaction = Transaction()
        transaction.transaction_id = __id_generator__()
        transaction.account_number = random.randint(1, 50)
        transaction.transaction_reference = __id_generator__()
        transaction.transaction_datetime = datetime.datetime.now().isoformat()
        transaction.amount = random.randint(1, 100)
        data_obj.data.append(transaction)
    return data_obj


"""schema"""
class Transaction(Message):
    transaction_id = StringField(field_number=1, required=True)
    account_number = Int32Field(field_number=2, required=True)
    transaction_reference = StringField(field_number=3, required=True)
    transaction_datetime = StringField(field_number=4, required=True)
    amount = Int32Field(field_number=5, required=True)

class Details(Message):
    data = MessageField(field_number=1, repeated=True, message_cls=Transaction)


for _ in range(500):
    data_obj = Details()
    details = generate_random_1000_transaction_data(data_obj)
    print(details)
    data = details.encode_to_bytes()
    producer.send('mytopic', value=data)
    sleep(3)

