import requests

# Base URL of your FastAPI service
BASE_URL = "http://localhost:8000"

# Your JWT token (you need to get this from login/authentication first)
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIrNzkzMTU0MjI3MjAiLCJleHAiOjE3NDY0ODA5MDd9.DtRb3D1t1BMCeBQkOWGVofOxbPrrd5zN0_7OomLvT6w"

# Request headers
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Request payload
payload = {
  "channels": [
    "varlamov"
  ],
  "n_messages": 3,
  "keywords": [
  ]
}

# Make the request
response = requests.post(
    f"{BASE_URL}/messages/",
    headers=headers,
    json=payload
)

# Print the response
if response.status_code == 200:
    messages = response.json()["messages"]
    print("Messages received:")
    for msg in messages:
        print(f"\nChannel: {msg['channel_name']}")
        print(f"Time: {msg['datetime']}")
        print(f"Text: {msg['text']}")
        if msg['photo']:
            print(f"Photo: {msg['photo']}")
        if msg['caption']:
            print(f"Caption: {msg['caption']}")
else:
    print(f"Error: {response.status_code}")
    print(response.json())
