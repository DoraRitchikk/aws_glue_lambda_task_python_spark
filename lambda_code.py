import json
import boto3
import requests
import urllib.parse

# Read data from S3


s3 = boto3.client("s3")
client = boto3.client("kinesis")


def lambda_handler(event, context):
    print("Recieved event : " + json.dumps(event, indent=2))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    airline_str = str(key)
    airline_name = airline_str[airline_str.find('=') + 1: airline_str.find('/part')]
    response = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

    dataFromJson = json.loads(response)

    key = 'vutH4PPMSRSbQfYjXRksA'
    url = 'https://www.carboninterface.com/api/v1/estimates'
    headers = dict(Authorization=f'Bearer {key}')
    data = dict(type='flight', passengers=2,
                legs=[dict(departure_airport=dataFromJson['Origin'], destination_airport=dataFromJson['Dest'])])

    pytres = requests.post(url, json=data, headers=headers)

    resp_dict = pytres.json()
    res_data = resp_dict.get('data')
    res_attr = res_data.get('attributes')
    res_legs = res_attr.get('carbon_kg')

    dataFromJson['Airline'] = airline_name
    dataFromJson['carbon_emission'] = res_legs

    toJson = json.dumps(dataFromJson)
    response = client.put_record(
        StreamName='airports_info_test_dritchik',
        Data=toJson,
        PartitionKey='Key'
    )
