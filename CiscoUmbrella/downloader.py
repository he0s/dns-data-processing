#!/usr/bin/env python

import argparse
import csv
import json
import logging
import os
import re
import sys
from urllib.request import urlretrieve
import zipfile

from datetime import date, timedelta
from itertools import product
from multiprocessing import Pool, cpu_count

from kafka import KafkaProducer


URL='http://s3-us-west-1.amazonaws.com/umbrella-static'
FILE_EXT='csv.zip'

DEST_PATH='/tmp/lists'

LISTS_CONFIG = {
    'top-1m': {
        'dir': 'top_1m',
        'topic_name': 'cisco-topic',
        'field_name': 'domain'
    },
    'top-1m-TLD': {
        'dir': 'top_1m_tld',
        'topic_name': 'cisco-tld-topic',
        'field_name': 'tld'
    }
}

KAFKA_SERVER='localhost'
KAFKA_PORT=19092

logging.basicConfig(**{
    "level": logging.INFO,
    "format": (
        "[%(asctime)s] %(levelname)s %(message)s"
    ),
    "datefmt": "%H:%M:%S",
    "stream": sys.stdout
})

logger = logging.getLogger(__name__)


def download_file(config):

    download_date, list_name = config

    if download_date == date.today():
        down_url = "{}/{}.{}".format(
            URL,
            list_name,
            FILE_EXT
        )
    else:
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
    
    dir_name = LISTS_CONFIG[list_name]['dir']

    out_file = os.path.join(
        DEST_PATH,
        dir_name,
        file_name
    )

    metadata_dict = {}
    if not os.path.exists(out_file):
        print("Downloading: %s" % down_url)
        try:
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

            result = (out_file, file_name, metadata_dict, list_name)

        except:
            logger.error("Can't process file {}".format(
                    down_url
                )
            )

            result = ()
    else:
        print("Skipping, %s already exists" % out_file)

        with open("{}.meta".format(out_file), 'r') as f:
            metadata_dict = json.load(f)

        result = (out_file, file_name, metadata_dict, list_name)

    return result


# Taken from https://stackoverflow.com/a/70426202
def gen_data(start_date, end_date):
    curr_date = start_date

    while curr_date <= end_date:
        yield curr_date
        curr_date += timedelta(days=1)


def process_file(config):

    file_obj, kafka_topic = config[0], config[1]

    if file_obj:
        producer = KafkaProducer(bootstrap_servers='{}:{}'.format(KAFKA_SERVER, KAFKA_PORT), compression_type='gzip')

        file_path, file_name, file_metadata, list_name = file_obj

        logger.info("Processing: {}".format(file_name))
        field_name = LISTS_CONFIG[list_name]['field_name']

        with zipfile.ZipFile(file_path) as zf:
            with zf.open("{}.{}".format(list_name,'csv')) as f:
                for line in f:
                    line = line.decode('utf-8')

                    message = json.dumps(
                        {
                            'download_date': file_metadata['Date'],
                            'date': re.search('([0-9]{4}-[0-9]{2}-[0-9]{2})', file_name).group(),
                            'produce_date': file_metadata['Last-Modified'],
                            'rank': str(line).split(',')[0],
                            field_name: str(line.strip()).split(',')[1]
                        }
                    ).encode('utf-8')

                    producer.send(kafka_topic, b'%s' % message)

        logger.info("Processing finished: {}".format(file_name))

    else:
        logger.info("File object is empty, nothing to process")


def main():

    get_common = False

    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--list-name", dest="list_name", required=True,
                        default=False,
                        choices=['top-1m', 'top-1m-TLD'], action="store",
                        help="A Cisco Umbrella list to download.")
    parser.add_argument("-s", "--start-date", dest="start_date", required=True,
                        default=False, action="store",
                        help="Start of date range, format: YYYY-MM-DD")
    parser.add_argument("-e", "--end-date", dest="end_date", required=False,
                        default=date.today(), action="store",
                        help="End of date range, format: YYYY-MM-DD")

    args, other = parser.parse_known_args()

    start_date = date.fromisoformat(args.start_date)
    end_date = args.end_date
    list_name = args.list_name

    path_name = LISTS_CONFIG[list_name]['dir']

    if not os.path.exists(os.path.join(DEST_PATH, path_name)):
        os.makedirs(os.path.join(DEST_PATH, path_name))

    dates_list = [dd for dd in gen_data(start_date, end_date)]

    # files_list = [download_file((date, list_name)) for date in dates_list]

    with Pool(int(cpu_count()/2)) as pl:
        files_list = pl.map(download_file, product(dates_list, [list_name], repeat=1))
        pl.map(process_file, product(files_list, [LISTS_CONFIG[list_name]['topic_name']], repeat=1))


if __name__ == '__main__':
    main()
