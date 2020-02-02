import boto3
import logging
import uuid
from botocore.exceptions import ClientError
from pprint import pprint
from tqdm import tqdm
from collections import deque

logger = logging.getLogger(__name__)

s3_client = boto3.client('s3',
                         aws_access_key_id='123',
                         aws_secret_access_key='123',
                         endpoint_url='http://localhost:4572/')

TEST_BUCKET = 'testbucket'
KEYS_COUNT = 1000000


def main():
    try:
        buckets = s3_client.list_buckets()
        logger.info(f'{pprint(buckets)}')
        bucket_list = buckets['Buckets']
        if TEST_BUCKET not in bucket_list:
            bucket_creation_response = s3_client.create_bucket(Bucket=TEST_BUCKET)
            logger.debug(f'Bucket Created :: {bucket_creation_response}')
    except ClientError as e:
        logger.exception(f'Unable to fetch buckets list : {e}')

    all_keys = []
    logger.info(f'Generating Keys...')
    # create initial 1000 folders
    for i in range(100):
        all_keys.append(uuid.uuid4().hex)

    depth = 2
    i = 0
    while i < depth:
        print(i)
        new_key_set = []
        for k in all_keys:
            for j in range(100):
                if i == depth-1:
                    new_key_set.append(k + "/" + uuid.uuid4().hex + ".txt")
                else:
                    new_key_set.append(k + "/" + uuid.uuid4().hex)
        all_keys.clear()
        all_keys.extend(new_key_set)
        i += 1

    logger.info(f'Adding keys to bucket :: {TEST_BUCKET}')

    for i in tqdm(range(len(all_keys))):
        s3_client.put_object(Bucket=TEST_BUCKET, Key=all_keys[i], Body=bytes(uuid.uuid4().hex, 'utf-8'))


if __name__ == '__main__':
    main()
