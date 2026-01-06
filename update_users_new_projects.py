#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Converted from Jupyter notebook:
  Sample Script - Adding Projects for MedSync Users.ipynb

Notes:
- Markdown cells are preserved as comments.
- Code cells are preserved in order with cell separators.
"""

# ---- Code cell 1 ----
# Setup

import requests, pandas as pd, json
import datetime, pytz
from pandas import json_normalize
from datetime import date, timedelta, datetime
import os, glob, pandas as pd, numpy as np, requests, json, pygsheets, tempfile

bearer = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IkJMcWhkMUR2X2xvaktYSHU0TFZMUSJ9.eyJvcmdhbml6YXRpb25JZCI6ImVmOWZkNzA2LWNmYmYtNDhlNy1iOTdlLTNiM2M4ZmUzMWZmYSIsInByb2plY3RJZHMiOlsiOTA3MzZlMjAtNTg4YS00MWY2LWIxYWEtMjE4MTE4NmRjZDY5IiwiY2Y5ZTQ4OTEtNTVjZi00YWY1LTlkOGEtYjNiZTY4NWFlYTIwIiwiMjVhNzA5NDctYTU2Ny00NjhjLWIwNTYtYTBkYzAwMDcyMWRlIiwiNmZmNTY1YTItMzI3Mi00MTM1LWI4ZDktZjM3ODU4NzhhNmZmIiwiNWFkNjA3ZDktNjAyMy00NjRlLWI4NDYtMmI5MDUzMTNmZDk5IiwiNjgwMTUxNjUtMDE3Zi00MDA5LWE5ODctZjk5Y2I1ZDFmMzE1IiwiNWY1MDlhMmEtNTJmMS00YzMwLTkzYTMtODI4MDIwOWUyNzQ3IiwiMDhhOWZmYjctNGFmNi00MTZiLWFjMTAtMGM5YWFhYjQ3ZDZkIiwiMzY3NzIwMWUtNmY0OC00Y2I1LTkyYzktMTViY2U5NGRkNjU5IiwiMTI2NzYxZmYtZDQ2Mi00NDY0LThiMTUtMjM2ZDgxMWQ2MjRjIiwiMWZkZDgxNDEtZjNmMi00MmQ4LTliYjAtMDRmNTQ5NGM2MGZlIiwiZDVhZDY2NjktOWJiNi00YTAzLWI1NGQtZDhhYTQzMzc4Y2FhIiwiYTE2YjQwODQtODhjNC00MjlkLTg1NDYtMzYxYjBlY2YzMWJlIiwiOTA2MzM1MmQtMTAxZC00Mzc0LWE1YWUtMzgzNjA4ZGZiZjU5IiwiMjdhZDdjNDktYTI0Yy00Yjc2LThkMjUtNjQ5NGZkOWExYWMwIiwiNDgyNDdiMDUtODJjNi00YzY3LWI3ZTgtNGJiODBlNzkyYmI5IiwiNDczNTY3MzUtNDhhOS00OGM4LWJjNTEtYWYyNzdkMmVlMTQ2IiwiNDc5MjBjNDQtMWUzZS00MDFlLWJmYjctNmFiYjlmOTI4OTRkIiwiN2UzMTFlZjQtOTgyMS00MzU0LTk4MWMtN2QzNzQ4M2ZhZDI5IiwiNTI2YmU5MTctZDcwYi00ODJkLTkwMmEtMDdkNjMyYzk4NTY4IiwiOTBjMTU3OWEtMmMwYS00NzhhLTk3ODctYzAxN2NjZTAzYzkxIiwiYThmY2VjYzktMzNkMC00NmM3LTllNzQtMGM3ZmEwMWEwMzMzIiwiN2I0MWExMDUtMzgyNi00MDgyLThhNjAtNjI0YTZjZDdmYjJmIiwiNDExNTQ4MDctMzNjNC00YzZlLWE1M2ItNjZkNDU1NGM2OGZiIiwiNTQ2ODdiMDEtOGM2My00NDIzLTg4M2QtNGRhNjBjYTdiMGVlIiwiOWQ3OTI5NmItMjQ3YS00MTQ1LTg2ZmEtY2EyZTdjOWU0ZTE5IiwiMWIyOTBlMmUtZjcxOS00MGJmLWIyM2UtOTA0ZjIzMDgzYThiIiwiMDU2NWZkZjQtYjMyZC00MjZkLWFlM2QtMGQ0N2NlMjQ5MmE4IiwiYWYwNjYzODYtMDlhNy00YTg5LTlkZTUtNzdhYzgxZmI3ZjRjIiwiZjFhNDEwZDMtZDkyYS00NDUwLWJjYWYtZDk4M2EzNzZmZTQ4IiwiY2ZmYWFlMTAtYWJjZS00NjRkLThmY2YtMGE3OGM5YTJmOWQwIiwiNDczYmY1MGQtY2Q5Yy00NjdmLWI2NDYtYjQ4OTcxMGM1NWZiIiwiMzM2NGQ5YzktZDgyZi00YmVmLWE3ODEtYmI3YWNjYWEzMTRlIiwiNjgzODQwZjctNTZhMS00MmQ0LTgwNGItNGJhZTJhZTlhODRhIiwiMGFlZjI4NDItMWE2Mi00YjJiLTgxYjMtZTMyOTJhOWRmMWVmIiwiOTZlMTRjODQtNTBiYi00YTRjLWI1YTctMWFlODFlNzI0NWQyIiwiYTAxMWFhMGYtN2IzZi00ZDk0LTk4OWYtNmM2MjEzZmQwNjgzIiwiYmZhNzgyOWItZWI1NC00MjRhLWFhYmUtMGU5YTYwNTdiOWY2IiwiOGE1OWQ1NTgtYmNkNi00ODMyLWIzYzMtNmFkZmU4MWMyZjg1IiwiNDQ5MjMwNDItNWFkNC00OTI5LWEzOWItOTEyMzc2MDc5MGJmIiwiZjZmN2EwYWMtNmM2OC00MGVjLTg5Y2ItYjQ2ODYyOTE2ZDRjIiwiOTc2ZTE1ZTAtNTc3Mi00OWU0LWIwNDctZjIzMDVjYzY5ZjlhIiwiNzg5MWY4OGQtOWM1Yi00MzA2LWE1NzQtYTRlYmViNDFkM2U2IiwiYzAzOGY4NjQtZWUzMS00MjA2LTk1OWYtNWJjZjBjZjE2ZDYzIl0sInVzZXJJZCI6IjRkZTQ0Y2NhLTk0ZmMtNGQ3ZC1hYWUwLTQ5YjQ3M2U0YjU0ZSIsImNvbm5lY3Rpb24iOnsibmFtZSI6IlVzZXJuYW1lLVBhc3N3b3JkLUF1dGhlbnRpY2F0aW9uIiwic3RyYXRlZ3kiOiJhdXRoMCJ9LCJpc3MiOiJodHRwczovL3hjdXJlcy1wYXRpZW50LXJlZ2lzdHJ5LXByb2QudXMuYXV0aDAuY29tLyIsInN1YiI6ImF1dGgwfDRkZTQ0Y2NhLTk0ZmMtNGQ3ZC1hYWUwLTQ5YjQ3M2U0YjU0ZSIsImF1ZCI6WyJwYXRpZW50LXJlZ2lzdHJ5LWFwaSIsImh0dHBzOi8veGN1cmVzLXBhdGllbnQtcmVnaXN0cnktcHJvZC51cy5hdXRoMC5jb20vdXNlcmluZm8iXSwiaWF0IjoxNzY3NzAwMzA2LCJleHAiOjE3Njc3MjE5MDYsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwiLCJhenAiOiJtdXdmc3d1TG0zSmRURmo2R3RDMlByNXJJVkM4SDVyNCIsInBlcm1pc3Npb25zIjpbIkFubm90YXRpb25fUmVhZCIsIkFubm90YXRpb25fV3JpdGUiLCJBcGlLZXlfTWFuYWdlIiwiQXBwbGljYXRpb25fUmVhZCIsIkNoZWNrbGlzdEl0ZW1EZWZpbml0aW9uX01hbmFnZSIsIkNoZWNrbGlzdHNNYW5hZ2UiLCJDb2hvcnRfQ3JlYXRlIiwiQ29ob3J0X0RlbGV0ZSIsIkNvaG9ydF9SZWFkIiwiQ29ob3J0X1N1YmplY3RfQXNzaWduIiwiQ29ob3J0X1N1YmplY3RfUmVhZCIsIkNvaG9ydF9VcGRhdGUiLCJDb2hvcnRfVXNlcl9Bc3NpZ24iLCJDb2hvcnRfVXNlcl9SZWFkIiwiQ29uc2VudF9SZWFkIiwiRGFzaGJvYXJkX1JlYWQiLCJEYXNoYm9hcmRfVXBkYXRlIiwiRGVtb2dyYXBoaWNRdWVyeV9DcmVhdGUiLCJEb2N1bWVudF9DcmVhdGUiLCJEb2N1bWVudF9EZWxldGUiLCJEb2N1bWVudF9SZWFkIiwiRG9jdW1lbnRfVXBkYXRlIiwiRmF4X1JlcXVlc3RfUmVhZCIsIkZoaXJfUmVhZCIsIkZoaXJfV3JpdGUiLCJPcmdhbml6YXRpb25fUmVhZCIsIlByb2dyYW1fUmVhZCIsIlByb2dyYW1fV3JpdGUiLCJQcm9qZWN0X0NyZWF0ZSIsIlByb2plY3RfUmVhZCIsIlByb2plY3RfVXBkYXRlIiwiUXVlcnlfQ3JlYXRlIiwiUXVlcnlfQ3JlYXRlX0J1bGsiLCJRdWVyeV9SZWFkIiwiUmVnaXN0cmF0aW9uUHJvZ3JhbV9SZWFkIiwiUmVnaXN0cmF0aW9uUHJvZ3JhbV9Xcml0ZSIsIlJlcXVlc3RfQ2VudGVyIiwiU3ViamVjdF9Db2hvcnRfQWNjZXNzX0FsbCIsIlN1YmplY3RfQ29ob3J0X0FjY2Vzc19VbmFzc2lnbmVkIiwiU3ViamVjdF9DcmVhdGUiLCJTdWJqZWN0X0RlbGV0ZSIsIlN1YmplY3RfSW1wb3J0IiwiU3ViamVjdF9Ob3RlcyIsIlN1YmplY3RfT3ZlcnZpZXciLCJTdWJqZWN0X1JlYWQiLCJTdWJqZWN0X1NlYXJjaCIsIlN1YmplY3RfU3VtbWFyeV9EZWxldGUiLCJTdWJqZWN0X1VwZGF0ZSIsIlN1bW1hcnlfQ2hlY2tsaXN0IiwiVGVtcGxhdGVfUmVhZCIsIlRlbXBsYXRlX1dyaXRlIiwiVXBsb2FkX0NkYSIsIlVzZXJfQ3JlYXRlIiwiVXNlcl9EZWxldGUiLCJVc2VyX1Blcm1pc3Npb25fTWFuYWdlIiwiVXNlcl9SZWFkIiwiVXNlcl9VcGRhdGUiXX0.WLoR7osP2_LCLmjMpWLjPjQBvwBoeC63VYR5-0mLe-vtXGZhKzlj3YOD6ZNBAQu76ZDL34iuWp9qxIB6g14dNIUf5rZtfsKw9bjEB9PRIeh7r4rsjwQE9RR3EP8ziF8epvadVHNaEFgor2ZJbDfNIlxOHqUob-Oovkam3IXX50vUZPP-RaDDpU7Urux35oJZWYnKkhsH0LoAgyK9wq9oUb-0LTjB8oGlzgX3OjbXOMWPeS3M3nv34mpHKpjR_Ka4Yv9Uzcu2nOaYrxGje7BdUXTQTHOE5o0I-mw6YgH0n2B883SKVIC-7IBnNCkcvUAWDKMWheFVbW2q-mjKxJfLxg'
projectId = '25a70947-a567-468c-b056-a0dc000721de'

headers = {
    'accept': 'application/json',
    'ProjectId': projectId,
    'Authorization': 'Bearer ' + bearer,
    'Content-Type': 'application/json'
}

# get existing user permissions
def get_user_permissions(user_id, bearer, projectId):
    url = 'https://partner.xcures.com/api/patient-registry/user/' + user_id + '?userId=' + user_id
    headers = {
        'accept': 'application/json',
        'ProjectId': projectId,
        'Authorization': 'Bearer ' + bearer,
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"{response.status_code}: {response.text}"
    
# update user permissions  
def update_user_permissions(user_id, user, coming, going, bearer, projectId):
    for i in coming:
        if i not in user['permissions']: user['permissions'].append(i)
    for j in going:
        if j in user['permissions']: user['permissions'].remove(j)
    url = 'https://partner.xcures.com/api/patient-registry/user/' + user_id
    headers = {
        'accept': 'application/json',
        'ProjectId': projectId,
        'Authorization': 'Bearer ' + bearer,
        'Content-Type': 'application/json'
    }
    payload = user
    response = requests.put(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return f"{response.status_code}: {response.text}"

# ---- Code cell 2 ----
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

# ---- Code cell 3 ----
# to add:
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
'c038f864-ee31-4206-959f-5bcf0cf16d63']

# Example: ['96e14c84-50bb-4a4c-b5a7-1ae81e7245d2','a011aa0f-7b3f-4d94-989f-6c6213fd0683','bfa7829b-eb54-424a-aabe-0e9a6057b9f6']

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
    user = get_user_permissions(sId, bearer, projectId)
    for j in projects:
        if j not in user['projectIds']:
            user['projectIds'].append(j)
    update_user_permissions(sId, user, [], [], bearer, projectId)

print()  # newline after progress bar
