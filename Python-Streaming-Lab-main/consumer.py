"""
consumer.py
----------------
This file contains the logic for a Consumer that continuously reads messages
from the queue and processes them.

The Consumer runs forever, just like a streaming system.
"""

import time
from queue import Queue

class Consumer:
    def __init__(self, q):
        """
        PARAMETERS:
        - q : the shared queue (simulated topic)

        TODO:
        - Store the queue.
        - Initialize any state variables you may want later,
          e.g. a running total for stateful computations.
        """
        self.q = q
        self.total_amount = 0.0  # stateful variable
        

    def start(self):
        """
        Main loop of the consumer.

        TODO:
        - Continuously call q.get() to receive events.
        - Print the event received.
        - Pass it to the process() function.
        """
        while True:
            event = self.q.get()   # blocks if queue is empty
            print(f"[CONSUMER] Received event: {event}")
            self.process(event)
            self.q.task_done()
    

    def process(self, event):
        """
        TODO:
        - Simulate some processing time using time.sleep().
        - Transform fields (e.g., convert amount to float).
        - Implement optional filtering conditions.
        - Update any state (example: running total).
        """
        time.sleep(0.5)

        # transform amount
        amount = float(event["amount"])
        self.total_amount += amount

        print(f"[CONSUMER] Processed amount: {amount}")
        print(f"[CONSUMER] Running total: {self.total_amount}")
   

# Debugging test

if __name__ == "__main__":
    """
    Run this alone to see consumer behavior.

    Note: It will block waiting for messages since no producer is running.
    """
    q = Queue()
    consumer = Consumer(q)
    consumer.start()
