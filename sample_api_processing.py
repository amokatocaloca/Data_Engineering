import requests

url = "https://api.tomorrow.io/v4/weather/realtime?location=almaty&apikey=MdtJ4G3OgyB7Dk7oitngojT7iNYpYGQ4"

headers = {
    "accept": "application/json",
    "accept-encoding": "deflate, gzip, br"
}

response = requests.get(url, headers=headers)

print(response.text)