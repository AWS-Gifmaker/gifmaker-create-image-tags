import boto3
from botocore.exceptions import ClientError
import uuid

dynamodb = boto3.resource('dynamodb')


class DynamoTableClient:
    def __init__(self, table_name):
        self.table = dynamodb.Table(table_name)

    def get_record(self, keys: dict):
        try:
            response = self.table.get_item(Key=keys)
        except ClientError as e:
            print(e.response['Error']['Message'])
            return -1
        else:
            if 'Item' in response.keys():
                return response['Item']
            else:
                return -1

    def put_record(self, record: dict):
        try:
            response = self.table.put_item(Item=record)
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            return response

    def update_record(self, record_keys: dict, name_values: dict):
        update_expression = "set"
        update_attributes = {}
        for key, value in name_values.items():
            update_expression += f" {key}=:{key},"  # note space prefix
            update_attributes[f":{key}"] = value
        update_expression = ','.join(update_expression.split(',')[:-1])

        print(update_expression)
        print(update_attributes)
        response = self.table.update_item(
            Key=record_keys,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=update_attributes,
            # UpdateExpression="set ready=:ready, info.psomething=:p, info.actors=:a",
            # ExpressionAttributeValues={
            #     ':ready': Decimal(rating),
            #     ':p': plot,
            #     ':a': actors
            # },
            ReturnValues="UPDATED_NEW"
        )
        return response


def main():

    dynamo_table_client = DynamoTableClient('gifs')

    labels = {'test2', 'test1', 'human'}
    record_keys = {'key': 'aaab'}
    record = dynamo_table_client.get_record(record_keys)
    if record == -1:
        # record not found
        print(f"Gif record with keys {record_keys} not found.")
        new_entry = {'ready': False, 'image_url': 'smth/smth', 'key': str(uuid.uuid4()), 'name': 'Why is this field here?', 'tags': labels, 'visits': 0}
        dynamo_table_client.put_record(new_entry)
        print(f"Created new roecord: {new_entry}")
    else:
        print(record)
        new_record_labels = record['tags'].union(labels)
        print(new_record_labels)

        dynamo_table_client.update_record(record_keys, {'tags': new_record_labels, 'ready': True})
