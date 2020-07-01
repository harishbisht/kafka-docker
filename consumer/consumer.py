from kafka import KafkaConsumer
from protobuf3.message import Message
from protobuf3.fields import StringField, Int32Field, MessageField


consumer = KafkaConsumer(
    'mytopic',
     bootstrap_servers=['kafka-2:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='mytopic_group')


"""schema"""
class Transaction(Message):
    transaction_id = StringField(field_number=1, required=True)
    account_number = Int32Field(field_number=2, required=True)
    transaction_reference = StringField(field_number=3, required=True)
    transaction_datetime = StringField(field_number=4, required=True)
    amount = Int32Field(field_number=5, required=True)

class Details(Message):
    data = MessageField(field_number=1, repeated=True, message_cls=Transaction)



details = Details()


#listener
for message in consumer:
    message = message.value
    details.parse_from_bytes(message)

    #aggregating the results
    accountno_balance_map = {}
    for d in details.data:
        if  d.account_number not in accountno_balance_map:
            accountno_balance_map[d.account_number] = 0
        accountno_balance_map[d.account_number] += d.amount

    # logging account_number and balance
    print("------------------------------------------")
    print("account number and balance")
    print(list(accountno_balance_map.items()))

