import datetime
import json
import random
import time

from cc_config import cc_config, sr_config

from confluent_kafka import Producer


class POSEvent(object):
    def __init__(self, retail_store_id, workstation_id, sequence_no,
                business_ts, operator_id, item_id, item_desc,
                unit_price, discount, tax):
        self.retail_store_id = retail_store_id
        self.workstation_id = workstation_id
        self.sequence_no = sequence_no
        self.business_ts = business_ts
        self.operator_id = operator_id
        self.item_id = item_id
        self.item_desc = item_desc
        self.unit_price = unit_price
        self.discount = discount
        self.tax = tax

def pos_event():
    now = datetime.datetime.now()
    rounded_now = str(now.replace(microsecond=0))
    retail_store_id = random.randint(166, 212)
    tills = ['TILL1' ,'TILL2' ,'TILL3' ,'TILL4' ,'TILL5' ,'TILL6', 'TILL7']
    workstation_id = random.choice(tills)
    sequence_no = random.randint(2, 9)
    business_ts = int(time.time() * 1000)
    operator_id = random.randint(201, 401)
    item_id = random.randint(22300, 22700)
    item_descs = ['Razor Men', 'Razor Women', 'Epilator', 'Shaving Cream',
                 'Bath Gel', 'Moisturiser', 'Face Scrub', 'After Shave',
                 'Eau De Cologne', 'Milk Clenaser', 'Deodorant', 'Shaving Gel']
    item_desc = random.choice(item_descs)
    unit_price = round(random.uniform(1.50, 7.80), 2)
    discount = round((unit_price * 0.05), 2)
    tax = round((unit_price * 0.02), 2)

    pos_event = POSEvent(
        retail_store_id = retail_store_id,
        workstation_id = workstation_id,
        sequence_no = sequence_no,
        business_ts = business_ts,
        operator_id = operator_id,
        item_id = item_id,
        item_desc = item_desc,
        unit_price = unit_price,
        discount = discount,
        tax = tax
    )

    return(pos_event)

def delivery_report(err, event):
    if err is not None:
        print(f"Delivery failed on event for {event.value().decode('utf8')}: {err}")
    else:
        print(f"Event {event.value().decode('utf8')} produced to {event.topic()}")

def posevent_to_dict(posevent):
    return dict(
        retail_store_id=posevent.retail_store_id,
        workstation_id=posevent.workstation_id,
        sequence_no=posevent.sequence_no,
        business_ts=posevent.business_ts,
        operator_id=posevent.operator_id,
        item_id=posevent.item_id,
        item_desc=posevent.item_desc,
        unit_price=posevent.unit_price,
        discount=posevent.discount,
        tax=posevent.tax
    )

def main():
    topic = "posrecord_noschema"
    producer = Producer(cc_config)

    try:
        while True:
            device_data = posevent_to_dict(pos_event())
            producer.produce(topic=topic,
                             value=json.dumps(device_data),
                             on_delivery=delivery_report)
            time.sleep(2)
            producer.flush()
    except Exception as e:
        print(e)
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
