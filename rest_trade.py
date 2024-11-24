import hashlib
import base64
import hmac
import time
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.environ.get('API_KEY')
private_key = os.environ.get('PRIVATE_KEY')

# Public request: Get market depth for XRP/USD
url_public = "https://api.kraken.com/0/public/Depth?pair=XRPUSD"

response = requests.get(url_public)
response = response.json()

ask_price = float(response['result']['XXRPZUSD']['asks'][0][0])
bid_price = float(response['result']['XXRPZUSD']['bids'][0][0])

print(f"Ask Price: {ask_price}")
print(f"Bid Price: {bid_price}")

buy_in_delta = 0.002
target_delta = 0.003
target_buy_price = ask_price - (buy_in_delta * ask_price)
target_sell_price = target_buy_price + (target_delta * target_buy_price)

print(f"Target Buy Price: {round(target_buy_price, 5)}")
print(f"Target Sell Price: {round(target_sell_price, 5)}")

# Example of placing an order
url_order = "https://api.kraken.com/0/private/AddOrder"
api_path_order = "/0/private/AddOrder"
nonce = str(int(time.time() * 1000))

# Payload for placing an order
order_payload = {
    "nonce": nonce,
    "ordertype": "limit",
    "type": "buy",
    "volume": "20",
    "pair": "XRPUSD",
    "price": str(round(target_buy_price, 5)),
    "close": {"ordertype": "limit", "price": str(round(target_sell_price, 5))},
}
encoded_order_payload = json.dumps(order_payload)

# Generate the signature for placing an order
postdata_order = nonce + encoded_order_payload
postdata_order_hash = hashlib.sha256(postdata_order.encode()).digest()
message_order = api_path_order.encode() + postdata_order_hash
signature_order = hmac.new(base64.b64decode(private_key), message_order, hashlib.sha512)
api_sign_order = base64.b64encode(signature_order.digest())

# Set headers for placing an order
headers_order = {
    'API-Key': api_key,
    'API-Sign': api_sign_order.decode(),
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# Send the request to place an order
response_order = requests.post(url_order, headers=headers_order, data=encoded_order_payload)

print(response_order.text)
