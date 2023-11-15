import boto3
import argparse
import json


parser = argparse.ArgumentParser(description='Load DynamoDB Table with values')
parser.add_argument('--profile', type=str, required=True, help='Specify AWS Profile to use')
parser.add_argument('--json', type=str, required=True, help='Specify JSON file for loading data in DDB')
parser.add_argument('--config', type=str, required=True, help='Specify config file')
args = parser.parse_args()

with open(args.config, "r") as jsonfile:
    data = json.load(jsonfile)
    print("Read successful")
print(data)

ddbtable = data['ddb']['tableName']
partitionKeyName = data['ddb']['partitionKeyName']
partitionKeyValueField = data['ddb']['partitionKeyValueField']
# print(ddbtable)
session = boto3.session.Session(profile_name=args.profile)
dynamodb = session.resource('dynamodb')
table = dynamodb.Table(ddbtable)


f = open(args.json)
data = json.load(f)

message = {}
message[partitionKeyName] = partitionKeyValueField
message['values'] = data
topics = {}
topics['Item'] = message
result = table.put_item(**topics)
print(result)


