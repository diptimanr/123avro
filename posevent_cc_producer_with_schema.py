import datetime
import random
import time

from cc_config import cc_config, sr_config

from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer


class POSEvent(object):
    def __init__(self, store_id, workstation_id, operator_id,
                 item_id, item_desc, unit_price,txn_date):
        self.store_id = store_id
        self.workstation_id = workstation_id
        self.operator_id = operator_id
        self.item_id = item_id
        self.item_desc = item_desc
        self.unit_price = unit_price
        self.txn_date = txn_date

def pos_event():
    now = datetime.datetime.now()
    rounded_now = str(now.replace(microsecond=0))
    store_id = random.randint(166, 212)
    tills = ['TILL1' ,'TILL2' ,'TILL3' ,'TILL4' ,'TILL5' ,'TILL6', 'TILL7']
    workstation_id = random.randint(10, 18)
    operator_id = random.choice(tills)
    item_id = random.randint(22300, 22700)
    item_descs = ['Razor Men', 'Razor Women', 'Epilator', 'Shaving Cream',
                  'Bath Gel', 'Moisturiser', 'Face Scrub', 'After Shave',
                  'Eau De Cologne', 'Milk Clenaser', 'Deodorant', 'Shaving Gel']
    item_desc = random.choice(item_descs)
    unit_price = round(random.uniform(1.50, 7.80), 2)
    txn_date = int(time.time() * 1000)

    pos_event = POSEvent(
        store_id = store_id,
        workstation_id = workstation_id,
        operator_id = operator_id,
        item_id = item_id,
        item_desc = item_desc,
        unit_price = unit_price,
        txn_date = txn_date
    )
    return(pos_event)

def delivery_report(err, event):
    if err is not None:
        print(f"Delivery failed on event for {event.key().decode('utf8')}: {err}")
    else:
        print(f"PoS Event {event.key().decode('utf8')} produced to {event.topic()}")

def posevent_to_dict(posevent, ctx):
    return dict(
        store_id=posevent.store_id,
        workstation_id=posevent.workstation_id,
        operator_id=posevent.operator_id,
        item_id=posevent.item_id,
        item_desc=posevent.item_desc,
        unit_price=posevent.unit_price,
        txn_date=posevent.txn_date
    )

def main():
    topic = "posrecord"
    schema = "avro/posevent.avsc"

    with open(schema) as f:
        schema_str = f.read()
    producer = Producer(cc_config)
    string_serializer = StringSerializer('utf_8')
    schema_registry_client = SchemaRegistryClient(sr_config)
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     posevent_to_dict)
    try:
        while True:
            producer.produce(topic=topic,
                             key=string_serializer(pos_event().operator_id),
                             value=avro_serializer(pos_event(), SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            time.sleep(2)
            producer.flush()
    except Exception as e:
        print(e)
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
