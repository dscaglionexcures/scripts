# Use this script to update all users in a tenant when new projects are created
# First use case is MedSync as they have a large number of projects that grows weekly
# export XCURES_BEARER_TOKEN="PASTE_TOKEN_HERE" to set the bearer token from your CLI so you don't add a token to the script and it accidentally gets saved to the repo

import requests, pandas as pd, json
import datetime, pytz
from pandas import json_normalize
from datetime import date, timedelta, datetime
import os, glob, pandas as pd, numpy as np, requests, json, pygsheets, tempfile

# Bearer token handling (expects token exported to environment)
def get_bearer_token() -> str:
    token = os.environ.get("XCURES_BEARER_TOKEN")
    if not token:
        raise RuntimeError(
            "XCURES_BEARER_TOKEN is not set. "
            "Run: export XCURES_BEARER_TOKEN='your_token_here'"
        )
    return token


headers = {
    'accept': 'application/json',
    'Authorization': 'Bearer ' + get_bearer_token(),
    'Content-Type': 'application/json'
}

# get existing user permissions
def get_user_permissions(user_id):
    url = 'https://partner.xcures.com/api/patient-registry/user/' + user_id + '?userId=' + user_id
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer ' + get_bearer_token(),
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"{response.status_code}: {response.text}"
    
# update user permissions  
def update_user_permissions(user_id, user, coming, going):
    for i in coming:
        if i not in user['permissions']: user['permissions'].append(i)
    for j in going:
        if j in user['permissions']: user['permissions'].remove(j)
    url = 'https://partner.xcures.com/api/patient-registry/user/' + user_id
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer ' + get_bearer_token(),
        'Content-Type': 'application/json'
    }
    payload = user
    response = requests.put(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return f"{response.status_code}: {response.text}"

# Get all users

all_responses = []
pgNumber = 1
pgSize = 25
pgNumberStr = str(pgNumber)
url = f'https://partner.xcures.com/api/patient-registry/user?pageSize=25&pageNumber={pgNumberStr}&hasActiveFilter=false&numberOfActiveFilters=0'

response = requests.get(url, headers=headers)
data = response.json()
all_responses.extend(data['results'])
totalCount = response.json()['totalCount']
while pgNumber * pgSize < totalCount:
    pgNumber = pgNumber + 1
    pgNumberStr = str(pgNumber)
    url = f'https://partner.xcures.com/api/patient-registry/user?pageSize=25&pageNumber={pgNumberStr}&hasActiveFilter=false&numberOfActiveFilters=0'
    response = requests.get(url, headers=headers)
    data = response.json()
    all_responses.extend(data['results'])
    
df = pd.DataFrame(all_responses)
df


# Include the ProjectIds to update each user with.
# Example: ['96e14c84-50bb-4a4c-b5a7-1ae81e7245d2','a011aa0f-7b3f-4d94-989f-6c6213fd0683','bfa7829b-eb54-424a-aabe-0e9a6057b9f6']
projects = [
'90736e20-588a-41f6-b1aa-2181186dcd69',
'cf9e4891-55cf-4af5-9d8a-b3be685aea20',
'25a70947-a567-468c-b056-a0dc000721de',
'6ff565a2-3272-4135-b8d9-f3785878a6ff',
'5ad607d9-6023-464e-b846-2b905313fd99',
'68015165-017f-4009-a987-f99cb5d1f315',
'5f509a2a-52f1-4c30-93a3-8280209e2747',
'08a9ffb7-4af6-416b-ac10-0c9aaab47d6d',
'3677201e-6f48-4cb5-92c9-15bce94dd659',
'126761ff-d462-4464-8b15-236d811d624c',
'1fdd8141-f3f2-42d8-9bb0-04f5494c60fe',
'd5ad6669-9bb6-4a03-b54d-d8aa43378caa',
'a16b4084-88c4-429d-8546-361b0ecf31be',
'9063352d-101d-4374-a5ae-383608dfbf59',
'27ad7c49-a24c-4b76-8d25-6494fd9a1ac0',
'48247b05-82c6-4c67-b7e8-4bb80e792bb9',
'47356735-48a9-48c8-bc51-af277d2ee146',
'47920c44-1e3e-401e-bfb7-6abb9f92894d',
'7e311ef4-9821-4354-981c-7d37483fad29',
'526be917-d70b-482d-902a-07d632c98568',
'90c1579a-2c0a-478a-9787-c017cce03c91',
'a8fcecc9-33d0-46c7-9e74-0c7fa01a0333',
'7b41a105-3826-4082-8a60-624a6cd7fb2f',
'1b290e2e-f719-40bf-b23e-904f23083a8b',
'0565fdf4-b32d-426d-ae3d-0d47ce2492a8',
'41154807-33c4-4c6e-a53b-66d4554c68fb',
'54687b01-8c63-4423-883d-4da60ca7b0ee',
'9d79296b-247a-4145-86fa-ca2e7c9e4e19',
'af066386-09a7-4a89-9de5-77ac81fb7f4c',
'f1a410d3-d92a-4450-bcaf-d983a376fe48',
'cffaae10-abce-464d-8fcf-0a78c9a2f9d0',
'473bf50d-cd9c-467f-b646-b489710c55fb',
'3364d9c9-d82f-4bef-a781-bb7accaa314e',
'683840f7-56a1-42d4-804b-4bae2ae9a84a',
'0aef2842-1a62-4b2b-81b3-e3292a9df1ef',
'96e14c84-50bb-4a4c-b5a7-1ae81e7245d2',
'a011aa0f-7b3f-4d94-989f-6c6213fd0683',
'bfa7829b-eb54-424a-aabe-0e9a6057b9f6',
'8a59d558-bcd6-4832-b3c3-6adfe81c2f85',
'44923042-5ad4-4929-a39b-9123760790bf',
'f6f7a0ac-6c68-40ec-89cb-b46862916d4c',
'976e15e0-5772-49e4-b047-f2305cc69f9a',
'7891f88d-9c5b-4306-a574-a4ebeb41d3e6',
'c038f864-ee31-4206-959f-5bcf0cf16d63',
'6aa0e547-f065-416a-a777-b1616fc87f48',
'715dd62d-99ba-42f4-8eee-83596e10c867',
'd33bd469-ae16-4080-a60d-619a4855b54a',
'b8b7d34f-4c00-4318-af57-0ad2d6249cea',
'09c39a0d-59cb-4ab0-93e4-4b73f825f0f1',
'2e3eb018-1949-48ba-bb21-879e95dd93a5',
'a86a83a5-399e-4c80-9d91-e565fc010a7b',
'30426858-c6a3-4adb-8127-9418502528b1',
'9f89a8ae-694f-4b96-a16a-de7f975b3ec2',
'52fa4a8e-8757-4c10-a30e-2806c037d512',
'42b9ddb2-6c13-4100-b947-0ee6036a99c2',
'1cb57724-eab3-4495-aebf-6da44e1ad563',
'36aa21a3-c160-4fd6-90c4-dec30a71fff7',
'19e55d7e-7d24-4e27-b4a6-eea1e2ce0f8c',
'a3d64c80-55d2-42c0-86de-fce6ceab740b'
]


# Progress bar setup
total_users = len(df)
bar_width = 30

for idx in range(total_users):
    # Simple progress bar + percentage
    progress = (idx + 1) / total_users if total_users else 1
    filled = int(bar_width * progress)
    bar = "█" * filled + "-" * (bar_width - filled)
    percent = progress * 100
    print(f"\rProgress: |{bar}| {percent:6.2f}% ({idx + 1}/{total_users})", end="", flush=True)

    sId = df['id'][idx]
    user = get_user_permissions(sId)
    for j in projects:
        if j not in user['projectIds']:
            user['projectIds'].append(j)
    update_user_permissions(sId, user, [], [])

print()  # newline after progress bar