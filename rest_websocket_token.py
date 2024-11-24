import hashlib
import base64
import hmac
import time
import requests
import json
import os

def get_token():
    api_key = os.environ.get('API_KEY')
    private_key = os.environ.get('PRIVATE_KEY')

    url_order = "https://api.kraken.com/0/private/GetWebSocketsToken"
    api_path_order = "/0/private/GetWebSocketsToken"
    nonce = str(int(time.time() * 1000))

    # Payload for placing an order
    order_payload = {
        "nonce": nonce,
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

    return response_order.json()['result']['token']
