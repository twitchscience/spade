#!/usr/bin/env python2.7
from __future__ import print_function
from collections import defaultdict
import sys
import csv
import time
import json
import hashlib
import gzip
import StringIO
import random
import socket
import os

import requests
import grequests
import boto.sqs


USAGE = './integration.py <ASG> <data csv> <hashes json file>'
EXIT_USAGE_ERROR = 1
EXIT_TEST_FAIL = 2
EXIT_ASG_ERROR = 3
EXIT_REQUEST_ERROR = 4


class Timer(object):
    def __init__(self, ctx):
        self.ctx = ctx

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print('{} took {} sec'.format(self.ctx,
                                      time.time() - self.start))


def error(msg):
    print(msg)


def errorout(msg=None, code=EXIT_USAGE_ERROR):
    if msg is None:
        print(USAGE)
    else:
        error(msg)
    sys.exit(code)


###########
#
# Asgard integration
#
if os.environ.get('ASGARD_URL'):
    ASGARD_URL = os.environ.get('ASGARD_URL')
else:
    errorout('Please set ASGARD_URL environment variable. Exiting...')
SHOW_URL = ASGARD_URL + '/{}/show/{}.json'
SHRINK_ASG_URL = ASGARD_URL + '/instance/terminateAndShrinkGroup?instanceId={}'


def instance_url(instance_id):
    return SHOW_URL.format('instance', instance_id)


def asg_url(asg_name):
    return SHOW_URL.format('autoScaling', asg_name)


def get_asg_instance(asg_name):
    res = requests.get(asg_url(asg_name)).json()
    instance_id = res['group']['instances'][0]['instanceId']
    return requests.get(instance_url(instance_id)).json()


def get_asg_instance_property(asg_name, prop):
    instance = get_asg_instance(asg_name)
    return instance['instance'].get(prop, None)


def downscale_asg(asg_name):
    instance = get_asg_instance(asg_name)
    url = SHRINK_ASG_URL.format(instance['instance']['instanceId'])
    res = requests.post(url)
    return res


##############
#
# Core
#
def request_parameters(csvline, verb):
    try:
        ip, msec, d = csvline
        base_parameters = {
            'headers': {
                'X-FORWARDED-FOR': ip,
                'X-ORIGINAL-MSEC': msec + ".000"
            },
            'timeout': 10.0
        }
        if verb == 'GET':
            if "data=" == d[:5]:
                d = d[5:]
            base_parameters['params'] = {
                'data': d,
            }
            return base_parameters
        if verb == 'POST':
            base_parameters['data'] = d
            return base_parameters
    except ValueError as e:
        print('Error: ', e, file=sys.stderr)
        print('Input: ', csvline, file=sys.stderr)
        return None


def get_requests(url, data_file):
    with open(data_file) as f:
        csvreader = csv.reader(f)
        for row in csvreader:
            params = request_parameters(row, 'GET')
            if params is not None:
                if random.random() > 0.5:
                    yield grequests.get(url, **request_parameters(row, 'GET'))
                else:
                    yield grequests.post(url, **request_parameters(row, 'POST'))


def send_requests(url, data_file, expected_results):
    results = defaultdict(int)
    for res in grequests.imap(get_requests(url, data_file), size=20):
        results[res.status_code] += 1
        if res.status_code != 204:
            error(res)
    return [results == expected_results, results]


def compute_length(keyname, bucket):
    # Stores the contents of the s3 gz file in a StringIO object and
    # then ungzip it in memory, sorts it and check the md5.
    # Rather this than having to fudge around with files on the OS
    gz_holder = StringIO.StringIO()
    key = bucket.get_key(keyname)
    if not key:
        print("Error: key not found " + keyname)
        return
    key.get_file(gz_holder)
    gz_holder.seek(0)
    gz = gzip.GzipFile(fileobj=gz_holder)
    count = 0
    for _ in gz:
        count += 1
    return count


def get_keys_to_verify():
    sqs = boto.sqs.connect_to_region("us-west-2")
    queue = sqs.get_queue("spade-compactor-integration")
    queue.set_message_class(boto.sqs.message.RawMessage)
    while True:
        msgs = queue.get_messages(visibility_timeout=30)
        if not msgs:
            return
        msg = msgs[0]
        try:
            upload = json.loads(msg.get_body())
            if "keyname" in upload and "tablename" in upload:
                queue.delete_message(msg)
                yield (upload["keyname"], upload["tablename"])
        except Exception as e:
            print(e)
            print("bad message: " + msg.get_body())
            return


def verify_output(lengths):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket('spade-compacter-integration')

    success = True
    ttl = int(time.time()) + 300
    ignore_tables = [
        "pageview",
    ]

    while len(lengths) > 0 and ttl - time.time() > 0:
        for k, table_name in get_keys_to_verify():
            keyname = "/".join(k.split("/")[1:])
            if table_name in ignore_tables:
                continue
            file_len = compute_length(keyname, bucket)
            expected_len = lengths.pop(table_name, None)
            if expected_len is None:
                success = False
                ignore_tables.append(table_name)
                msg = 'unexpected table:{} (file:{},hash:{})'
                print(msg.format(table_name, keyname, file_len))
            elif expected_len != file_len:
                success = False
                ignore_tables.append(table_name)
                msg = 'expected hash:{} for file:{}, got hash:{}'
                print(msg.format(expected_len, keyname, file_len))

        if len(lengths) > 0:
            time.sleep(10)

    if len(lengths) > 0:
        msg = 'Did not find the following tables: {}'
        print(msg.format(', '.join(lengths.keys())))
        msg = 'Ignored the following tables: {}'
        print(msg.format(', '.join(ignore_tables)))

    if success:
        success = len(lengths) == 0

    return success


def main(edge_asg_name, processor_asg_name, data_file, hashes):
    target_hostname = get_asg_instance_property(edge_asg_name, 'publicDnsName')
    addr = socket.gethostbyname(target_hostname)
    url = 'http://{}/'.format(addr)
    print('sending requests to: {}'.format(url))

    with Timer('Sending requests'):
        expected_results = {204: 19999}
        successful, results = send_requests(url, data_file, expected_results)
        if not successful:
            error("failed to successfully send all requests")
            error("expected {} {}'s got:".format(expected_results[204], 204))
            for k, v in results.iteritems():
                error("\t{}:\t{}".format(k, v))
            errorout('failed to successfully send requests', EXIT_REQUEST_ERROR)

    # Pretty critical, waiting 1 minute causes files to not be
    # processed. 2 is safe, 90s is probably the absolute minimum
    # that'd work
    wait_min = 1
    print('waiting {} minute....'.format(wait_min))
    time.sleep(60 * wait_min)

    # forces causes uploader to move files to s3
    res = downscale_asg(edge_asg_name)
    if not res.ok:
        msg = 'error downscaling asg {}: {}'
        errorout(msg.format(edge_asg_name, res.status_code), EXIT_ASG_ERROR)

    wait_min = 2
    print('waiting {} minute....'.format(wait_min))
    time.sleep(60 * wait_min)

    # forces causes uploader to move files to s3
    res = downscale_asg(processor_asg_name)
    if not res.ok:
        msg = 'error downscaling asg {}: {}'
        errorout(msg.format(processor_asg_name, res.status_code),
                 EXIT_ASG_ERROR)

    wait_min = 3
    print('waiting {} minute....'.format(wait_min))
    time.sleep(60 * wait_min)

    with Timer('Checking bucket'):
        if verify_output(hashes):
            print('tests passed successfully')
        else:
            errorout('errors during test', code=EXIT_TEST_FAIL)


if __name__ == '__main__':
    # see USAGE
    if len(sys.argv) < 4:
        errorout()
    hash_file = sys.argv[4]
    with open(hash_file, 'r') as f:
        hashes = json.loads(f.read())

    if not hashes:
        errorout('{} should contain valid hashes'.format(hash_file))

    main(sys.argv[1], sys.argv[2], sys.argv[3], hashes)
