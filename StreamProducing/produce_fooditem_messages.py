import random
import time
from time import sleep
import threading

from kafka import KafkaProducer

#producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))

separator = '|'
order_n = 0

def emit_message(sleep_time, order_number, item, count):

    sleep(sleep_time)

    rec = separator.join([str(int(float(time.time()) * 1000)), str(order_number), item, str(count)])
    print(rec)
    #producer.send('fish-n-chips-orders', rec)

    return

while True:
    """
    Keep producing records with a 
    variable delay.
    """
    for _ in range(0, random.randint(0, 5)):

        current_order_n = order_n
        order_n += 1

        portions = random.randint(1, 6)
        # create two threads that will emit messages
        rand_sleep_fish = random.randint(0, 300)
        thr_fish = threading.Thread(target=emit_message, args=(rand_sleep_fish, current_order_n, 'fish', portions))
        rand_sleep_chips = random.randint(0, 200)
        thr_chips = threading.Thread(target=emit_message, args=(rand_sleep_chips, current_order_n, 'chips', portions))

        thr_fish.start()
        thr_chips.start()
    
    print("Current Order Number: ", order_n - 1)

    sleep(random.randint(0, 120))