import requests
import pygsheets
from google.cloud import bigquery
import pandas as pd
import os
import io
from datetime import datetime, timedelta, date
import json


# Prepare Slack webhook to send message to
slack_webhook_url = '[YOUR_SLACK_WEBHOOK_URL]'

def send_slack_message(message):
    payload = {'text': message}
    headers = {'Content-type': 'application/json'}
    response = requests.post(slack_webhook_url, data=json.dumps(payload), headers=headers)
    return response.status_code == requests.codes.ok

#Prepare google authorization
gc_credential_file = "[YOUR_CREDENTIAL_FILE]"
gc = pygsheets.authorize(service_account_file = gc_credential_file)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_credential_file

#Create google sheet read-function
def read_sht_table(survey_url, sheet):
    sht = gc.open_by_url(survey_url)
    ws = sht.worksheet_by_title(sheet)
    df = ws.get_as_df(has_header = True, include_tailing_empty=False)
    data = pd.DataFrame(df)
    return data

# Get all the project information
df1 = read_sht_table("[GOOGLE_SHEET_URL]","Sheet1")

# Declare empty DataFrame to merge all the projects
final_df = pd.DataFrame()


# Prepare reference to the dataset
BigQuery_client = bigquery.Client()

# Prepare job_config and schema
table_id = "PROJECTS.TABLE"
job_config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField("chatfuel_fbid", "STRING"),
        bigquery.SchemaField("signed_up", "DATETIME"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("project_id", "NUMERIC"),
        bigquery.SchemaField("project_name", "STRING")
    ],
    autodetect=False,
    source_format=bigquery.SourceFormat.CSV,
    write_disposition="WRITE_TRUNCATE")

# Declare dataset and table to write to
dataset_ref = BigQuery_client.dataset('DATASET')
table_ref = dataset_ref.table("TABLE")

try:
    for i in range(0, len(df1)):
        # Get post response for each project
        post_url = "https://dashboard.chatfuel.com/graphql?opName=GenerateUsersDocument"
        post_headers = {
            "authorization": "Bearer [YOUR_CHATFUEL_TOKEN]",
            "content-type": "application/json",
        }

        bot_id = df1['hash'][i]
        post_data = {
            "operationName":"GenerateUsersDocument",
            "variables":{
                "botId":bot_id,
                "filter":"{\"operation\":\"and\",\"parameters\":[{\"name\":\"\",\"values\":[],\"operation\":\"is\",\"type\":\"system\"}]}", 
                "params":{
                    "desc":True,
                    "sortBy":"updated_date",
                    "fields":[
                        {"name":"signed up","type":"system"},
                        {"name":"status","type":"system"},
                    ]
                }
            },
            "query":"mutation GenerateUsersDocument($botId: String!, $params: ExportParams, $filter: String) {\n  generateUsersExportDocument(botId: $botId, params: $params, filter: $filter) {\n    generatedId\n    downloadUrl\n    __typename\n  }\n}\n"
        }
        post_response = requests.post(post_url, headers=post_headers, json=post_data)
        response_json = post_response.json()
        download_url = response_json["data"]["generateUsersExportDocument"]["downloadUrl"]
        file_url = "https://dashboard.chatfuel.com" + download_url

        result = requests.get(file_url)
        result.raise_for_status()
        result_df = pd.read_csv(io.StringIO(result.text))
        result_df = result_df[['chatfuel user id', 'signed up','status']]
        result_df.rename(columns={'chatfuel user id' : 'chatfuel_fbid', 'signed up' : 'signed_up'},inplace=True)
        result_df['signed_up'] = pd.to_datetime(result_df['signed_up'])
        result_df['signed_up'] = result_df['signed_up']  + timedelta(hours=8)
        result_df['project_id'] = df1['project_id'][i]
        result_df['project_name'] = df1['project_name'][i]
        final_df = pd.concat([final_df,result_df])

except:
    send_slack_message(df1['project_name'] + ' failed to download')


# Writes final_df into BQ
job = BigQuery_client.load_table_from_dataframe(final_df, table_ref, job_config=job_config, location="asia-east1")
job.result()  # Waits for table load to complete.
assert job.state == "DONE"