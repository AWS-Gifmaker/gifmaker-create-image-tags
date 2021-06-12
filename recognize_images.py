import base64
import os
import uuid
import cv2
import numpy as np
import boto3

from datetime import datetime
from urllib.parse import unquote_plus

from DynamoTableClient import DynamoTableClient

LOCAL_MODE = False

# reduce cost by limiting num of frames passes to AWS Rekognition
# TODO: move to param store?
MAX_FRAMES_USED = 2

img_rekognition_client = boto3.client('rekognition')
if not LOCAL_MODE:
    s3_client = boto3.client('s3')

    dynamo_table_client = DynamoTableClient('gifs')


def analyze_video(vid_path: str):
    print(f"START Analyzing new video, path: {vid_path}")

    cap = cv2.VideoCapture(vid_path)
    cap.set(cv2.CAP_PROP_POS_AVI_RATIO, 0)

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    vid_fps = int(cap.get(cv2.CAP_PROP_FPS))

    print("Vide stats:")
    print(f"total_frames: {total_frames}")
    print(f"frame_w: {frame_w}")
    print(f"frame_h: {frame_h}")
    print(f"vid_fps: {vid_fps}")
    print(f"DURATION: {total_frames / vid_fps}")

    used_frames_indices = get_used_frames_indices(total_frames)
    print(f"Using frames: {used_frames_indices}")

    performance_per_frame = []

    fc = 0
    detected_labels = set()
    while fc < total_frames:
        ret, img_buffer = cap.read()
        img_buffer = cv2.cvtColor(img_buffer, cv2.COLOR_BGR2RGB)
        if fc in used_frames_indices:
            frame_proc_start = datetime.now()
            print(f"Handling frame {fc}, time: {frame_proc_start}")

            ret, encoded_jpeg = cv2.imencode('.jpg', img_buffer)
            img_base_64 = base64.b64encode(encoded_jpeg)
            img_base_64_binary = base64.decodebytes(img_base_64)

            response = img_rekognition_client.detect_labels(
                Image={'Bytes': img_base_64_binary},
                MaxLabels=20,
                MinConfidence=0.5
            )

            print(f'Detected labels in vid {vid_path}, frame index {fc}')
            for label in response['Labels']:
                print(label['Name'] + ' : ' + str(label['Confidence']))
            detected_labels.update({label['Name'] for label in response['Labels']})

            frame_proc_end = datetime.now()
            proc_delta = (frame_proc_end - frame_proc_start).total_seconds()
            print(f"Frame {fc} handled, total time: {proc_delta} seconds")
            performance_per_frame.append(proc_delta)
        fc += 1

    cap.release()
    print(f'All detected labels: {detected_labels}')
    print(
        f"END, frames covered: {len(used_frames_indices)}/{total_frames}, average time per frame: {sum(performance_per_frame) / len(performance_per_frame)} seconds")
    return detected_labels


def get_used_frames_indices(vid_frame_count: int):
    """
    Select indices of frames used for image rekognition. Spreads used indices evenly over all frames.
    :param vid_frame_count:
    :return:
    """
    num_frames_used = min(vid_frame_count, MAX_FRAMES_USED)
    fractional_indices = np.linspace(0, vid_frame_count - 1, num_frames_used)
    return [int(i) for i in fractional_indices]


def print_file_tree():
    print(os.getcwd())
    for root, dirs, files in os.walk("../venv/lib/python3.8/site-packages", topdown=False):
        for name in files:
            print(os.path.join(root, name))
        for name in dirs:
            print(os.path.join(root, name))


def update_db_entry(detected_labels, object_key, source_bucket, source_bucket_region):
    print(f"Started DB update procedure")
    record_keys = {'key': object_key}
    record = dynamo_table_client.get_record(record_keys)

    if record == -1:
        # record not found
        print(f"Gif record with keys {record_keys} not found.")
        new_entry = {'ready': True,
                     'image_url': f"https://{source_bucket}.s3.{source_bucket_region}.amazonaws.com/{object_key}",
                     'key': object_key,  # str(uuid.uuid4()),
                     'name': 'Why is this field here?',
                     'tags': detected_labels,
                     'visits': 0}
        dynamo_table_client.put_record(new_entry)
        print(f"Created new record: {new_entry}")
    else:
        print(f"Updating existing GIF record with keys {record_keys}")
        print(f"Record found: {record}")
        if "tags" in record.keys() and type(record["tags"]) is set:
            new_record_labels = record['tags'].union(detected_labels)
        else:
            new_record_labels = detected_labels
        print(f"Final labels: {new_record_labels}")

        dynamo_table_client.update_record(record_keys, {'tags': new_record_labels, 'ready': True})


def lambda_handler(event: dict, context):
    print(f"Handler called, received event: {event}")
    for record in event['Records']:
        region = record['awsRegion']
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        print(f"Input info: bucket={bucket}, key={key}")

        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        print(f"Downloading gif object to: {download_path}")
        s3_client.download_file(bucket, key, download_path)
        print("Download completed.")

        print("Calling analyze method.")
        detected_labels = analyze_video(download_path)

        print("Updating labels in DynamoDB")
        db_object_key = '.'.join(key.split('.')[:-1])
        update_db_entry(detected_labels, db_object_key, bucket, region)

        print(f"Cleaning up tmp files from {download_path}")
        os.remove(download_path)

    print("Lambda handler closing")


def main():
    # file_path = "vids/vid1.mov"
    file_path = "vid111"
    analyze_video(file_path)


if __name__ == "__main__":
    main()
