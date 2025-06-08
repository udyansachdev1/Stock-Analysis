import requests

url = "http://127.0.0.1:5000/analyze"

data = {"text": "I love this product! It's amazing!"}
response = requests.post(url, json=data)

if response.status_code == 200:
    result = response.json()
    print(f"Sentiment: {result['sentiment']}")
    print(f"Sentiment Score: {result['score']}")
else:
    print(f"Error: {response.text}")
