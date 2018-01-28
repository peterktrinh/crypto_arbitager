# gdax.py
# Receive messages from the gdax Websocket Feed

from __future__ import print_function
import json
import base64
import hmac
import hashlib
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from auth import get_auth_headers
from pylimitbook.book import Book

class WebsocketClient(object):
    def __init__(self, url="wss://ws-feed.gdax.com", products=None,
                 message_type="subscribe", should_print=True, auth=False,
                 api_key="", api_secret="", api_passphrase="", channels=None):
        self.url = url
        self.products = products
        self.channels = channels
        self.type = message_type
        self.stop = False
        self.error = None
        self.ws = None
        self.thread = None
        self.auth = auth
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.should_print = should_print

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
        self.on_open()
        self.thread = Thread(target=_go)
        self.thread.start()

    def _connect(self):
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        if self.channels is None:
            sub_params = {'type': 'subscribe', 'product_ids': self.products}
        else:
            sub_params = {'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels}

        if self.auth:
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            message = message.encode('ascii')
            hmac_key = base64.b64decode(self.api_secret)
            signature = hmac.new(hmac_key, message, hashlib.sha256)
            signature_b64 = signature.digest().encode('base64').rstrip('\n')
            sub_params['signature'] = signature_b64
            sub_params['key'] = self.api_key
            sub_params['passphrase'] = self.api_passphrase
            sub_params['timestamp'] = timestamp

        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(sub_params))

        if self.type == "heartbeat":
            sub_params = {"type": "heartbeat", "on": True}
        else:
            sub_params = {"type": "heartbeat", "on": False}
        self.ws.send(json.dumps(sub_params))

    def _listen(self):
        while not self.stop:
            try:
                if int(time.time() % 5) == 0:
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        if self.type == "heartbeat":
            self.ws.send(json.dumps({"type": "heartbeat", "on": False}))
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def close(self):
        self.stop = True
        self.thread.join()

    def on_open(self):
        if self.should_print:
            print("-- Subscribed! --\n")

    def on_close(self):
        if self.should_print:
            print("\n-- Socket Closed --")

    def on_message(self, msg):
        if self.should_print:
            print(msg)
        if self.mongo_collection:  # dump JSON to given mongo collection
            self.mongo_collection.insert_one(msg)

    def on_error(self, e, data=None):
        self.error = e
        self.stop = True
        print('{} - data: {}'.format(e, data))


if __name__ == "__main__":
    import sys
    import gdax
    import time

    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            global book
            self.url = "wss://ws-feed.gdax.com/"
            self.channels = ['level2']
            self.products = ["BTC-USD"]
            book = Book()


        def on_message(self, msg):
            # If first message, init order book
            if msg['type'] == 'snapshot':
                for each in msg['asks']:
                    # event, symbol, exchange, id_num, qty, price, timestamp
                    input = ','.join(['A', msg['product_id'], 'GDAX', each[0], each[1], each[0], '0'])
                    book.ask(input)
                for each in msg['bids']:
                    input = ','.join(['B', msg['product_id'], 'GDAX', each[0], each[1], each[0], '0'])
                    book.bid(input)
            # Otherwise update order book
            elif msg['type'][0] == 'l2update':
                change = msg['changes'][0]
                if change[0] == 'sell':
                    input = ','.join(['A', msg['product_id'], 'GDAX', change[1], change[2], change[1], msg['time']])
                    book.ask(input)
                elif change[0] == 'buy':
                    input = ','.join(['B', msg['product_id'], 'GDAX', change[1], change[2], change[1], msg['time']])
                    book.bid(input)

        def on_close(self):
            print('bids are:')
            count = 0
            if book.bids != None and len(book.bids) > 0:
                for k, v in book.bids.price_tree.items(reverse=True):
                    print('%s' % v)
                    count += 1
                    if count>10:
                        break
            print('asks are:')
            count = 0
            if book.asks != None and len(book.asks) > 0:
                for k, v in book.asks.price_tree.items(reverse=False):
                    print('%s' % v)
                    count += 1
                    if count>10:
                        break
            print("-- Goodbye! --")


    wsClient = MyWebsocketClient()
    wsClient.start()
    print(wsClient.url, wsClient.products)
    try:
        while True:
            # print("\nMessageCount =", "%i \n" % wsClient.message_count)
            time.sleep(1)
    except KeyboardInterrupt:
        wsClient.close()

    if wsClient.error:
        sys.exit(1)
    else:
        sys.exit(0)