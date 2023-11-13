import requests
from dotenv import load_dotenv

load_dotenv()
job_id = "877692463529789"
server_h = "adb-3644312116471444.4.azuredatabricks.net"
access_token = "dapi8dc3b34593d6c48295f29223e9609b72-3"

url = f'https://{server_h}/api/2.0/jobs/run-now'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}

data = {
    'job_id': job_id
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print('Job run successfully triggered')
else:
    print(f'Error: {response.status_code}, {response.text}')