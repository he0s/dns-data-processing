#!/usr/bin/env python

import csv
import json
import os
import re
from urllib.request import urlretrieve
import wget
import zipfile

from datetime import date, timedelta

from kafka import KafkaProducer


URL='http://s3-us-west-1.amazonaws.com/umbrella-static'
TOP_1M_PREF='top-1m'
TOP_1M_TLD_PREF='top-1m-TLD'
FILE_EXT='csv.zip'

DEST_PATH='/tmp/lists'
TOP_1M_DIR='top_1m'
TOP_1M_TLD_DIR='top_1m_tld'

KAFKA_SERVER='localhost'
KAFKA_PORT=19092
KAFKA_TOPIC='cisco-topic'


def download_file(download_date, list_name=TOP_1M_PREF, dir_name=TOP_1M_DIR):

    down_url = "{}/{}-{}.{}".format(
        URL,
        list_name,
        download_date,
        FILE_EXT

    )

    file_name = "{}-{}.{}".format(
        list_name,
        download_date,
        FILE_EXT
    )
    
    if list_name != TOP_1M_PREF:
        dir_name = TOP_1M_TLD_DIR
        # int_file = TOP_1M_TLD_PREF

    out_file = os.path.join(
        DEST_PATH,
        dir_name,
        file_name
    )

    metadata_dict = {}
    if not os.path.exists(out_file):
        print("Downloading: %s" % down_url)
        path, headers = urlretrieve(down_url, out_file)

        if headers:
            for name, value in headers.items():
                metadata_dict[name] = value

        with open("{}.meta".format(out_file), 'w') as f:
            json.dump(metadata_dict, f)
        # NOTE: It was the first version of the solution,
        # but I changed my mind since the urllib provides
        # more interesting approach of the file downloading,
        # that gives me some additional metadata of the file
        # that can be used for the data analysis purposes.
        # wget.download(down_url, out_file)
    else:
        print("Skipping, %s already exists" % out_file)

        with open("{}.meta".format(out_file), 'r') as f:
            metadata_dict = json.load(f)


    return (out_file, file_name, metadata_dict)

# Taken from https://stackoverflow.com/a/70426202
def gen_data(start_date, end_date):
    curr_date = start_date

    while curr_date <=end_date:
        yield curr_date
        curr_date += timedelta(days=1)


def main():
    producer = KafkaProducer(bootstrap_servers='{}:{}'.format(KAFKA_SERVER, KAFKA_PORT))

    if not os.path.exists(os.path.join(DEST_PATH, TOP_1M_DIR)):
        os.makedirs(os.path.join(DEST_PATH, TOP_1M_DIR))

    if not os.path.exists(os.path.join(DEST_PATH, TOP_1M_TLD_DIR)):
        os.makedirs(os.path.join(DEST_PATH, TOP_1M_TLD_DIR))

    start_date = date.fromisoformat('2017-02-08')
    end_date = date.today()

    # print(start_date, stop_date)
    dates_list = [dd for dd in gen_data(start_date, end_date)][:5]

    files_list = [download_file(date) for date in dates_list]

    # print(files_list)

    for file_path, file_name, file_metadata in files_list:
        with zipfile.ZipFile(file_path) as zf:
            # TODO: replace the file name below to a variable depending
            # on a domain list processing.
            with zf.open("{}.{}".format(TOP_1M_PREF,'csv')) as f:
                for line in f:
                    line = line.decode('utf-8')

                    # print(str(line.strip()))
                    message = json.dumps(
                        {
                            'download_date': file_metadata['Date'],
                            'date': re.search('([0-9]{4}-[0-9]{2}-[0-9]{2})', file_name).group(),
                            'produce_date': file_metadata['Last-Modified'],
                            'rank': str(line).split(',')[0],
                            'domain': str(line.strip()).split(',')[1]
                        }
                    ).encode('utf-8')

                    producer.send(KAFKA_TOPIC, b'%s' % message)
    
    # files = list(map(download_file, date_list))
    # for dd in gen_data(start_date, end_date):


if __name__ == '__main__':
    main()