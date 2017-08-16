#!/usr/bin/env python2.7
"""Reload rows of data into acedb from edge through processor.

    Usage:
        replay.py START END [TABLE ... | --all-tables] --rsurl=<url>
                  [--processor-only] [--from-runtag=<runtag>]
                  [--poolsize=<size>] [--log=<level>]
        replay.py --help

    Arguments:
        START   timestamp of start of period in "%Y-%m-%d %H:%M:%S" format
                in PT
        END     timestamp of end of period in "%Y-%m-%d %H:%M:%S" format in PT
        TABLE   table[s] to reload into

    Options:
        --rsurl=<url>   `postgres://` style url to access the redshift database
        --processor-only    if present, skip the DB step
        --from-runtag=<runtag>
            if present, skip the Spark step and upload to DB from runtag
        --poolsize=<size>
            Size of pool for parallel ingester operations [default: 4]
        --log=<level>   the logging level [default: INFO]
        --all-tables
            if present, upload data to all database tables known to blueprint
"""
from base64 import b64encode
import datetime
import logging
import os
import subprocess
import sys
from multiprocessing.dummy import Pool

import boto3
import botocore.session
from docopt import docopt
import psycopg2
from pyspark import SparkConf, SparkContext
import pytz
import requests
from zlib import decompress, MAX_WBITS


LOGGER = None
PT = pytz.timezone('US/Pacific')
UTC = pytz.timezone('UTC')
EDGE_BUCKET = os.environ['EDGE_BUCKET']

# COPY_OPTS copied from ingester code
# might be worth verifying it when this script is used
COPY_OPTS = ('''removequotes delimiter '\t' gzip escape truncatecolumns ''' +
             '''roundec fillrecord compupdate off statupdate off '''
             '''emptyasnull acceptinvchars '?' trimblanks''')


def set_up_logging(args):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')
    global LOGGER
    LOGGER = logging.getLogger('')


def get_days(start, end):
    """Yield all the UTC days for the dates in [start, end]."""
    # 1-hour margin to allow for inaccuracy in file timestamps
    start = (start + datetime.timedelta(hours=-1)).astimezone(UTC)
    end = (end + datetime.timedelta(hours=1)).astimezone(UTC)
    duration = (end.date() - start.date()).days + 1
    for val in xrange(duration):
        yield (start.date() + datetime.timedelta(days=val)).strftime("%Y%m%d")


def spark_context():
    return SparkContext(conf=SparkConf().
                        set('spark.task.cpus', os.environ['CPUS_PER_WORKER']).
                        setAppName("Processor Replay"))


def s3_object_keys(start, end):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(EDGE_BUCKET)
    try:
        s3.meta.client.head_bucket(Bucket=EDGE_BUCKET)
    except botocore.exceptions.ClientError as e:
        # Create the bucket if it doesn't exist
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            s3.create_bucket(Bucket=EDGE_BUCKET, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    edge_objects = boto3.resource('s3').Bucket(EDGE_BUCKET).objects
    return [s.key
            for prefix in get_days(start, end)
            for s in edge_objects.filter(Prefix=prefix)
            if s.last_modified >= start and
            s.last_modified - datetime.timedelta(hours=1) < end]


def contents(key):
    s3_object = boto3.resource('s3').Object(EDGE_BUCKET, key)
    gzipped_text = s3_object.get()['Body'].read()
    # " | 16" means use a gzip header
    return decompress(gzipped_text, MAX_WBITS | 16)


def pipe_through_processor(run_tag, fragment_list):
    def fn(key_iter):
        processor = subprocess.Popen(
            ['/opt/science/replay/bin/run_spade_replay.sh', run_tag],
            stdin=subprocess.PIPE)
        if fragment_list is None:
            for key in key_iter:
                processor.stdin.write(contents(key))
        else:
            for key in key_iter:
                processor.stdin.writelines(
                    line for line in contents(key).splitlines(True)
                    if any(fragment in line for fragment in fragment_list))
        processor.stdin.close()
        processor.wait()
        if processor.returncode > 0:
            raise subprocess.CalledProcessError

    return fn


def replay_processor(start, end, run_tag, fragment_list):
    s3_keys = s3_object_keys(start, end)
    spark_context().\
        parallelize(s3_keys, len(s3_keys) / 100 + 1).\
        foreachPartition(pipe_through_processor(run_tag, fragment_list))


def get_tables_from_blueprint():
    return {row["EventName"]
            for row in requests.get(os.environ['BLUEPRINT_URL']).json()}


def s3_dir_exists(key):
    client = boto3.client('s3')
    keys = client.list_objects(
        Bucket=os.environ['COMPACTER_BUCKET'], Prefix=key)
    return 'Contents' in keys


def ingester_worker(table, start, end, rsurl, run_tag):
    LOGGER.info('starting %s', table)
    c = botocore.session.get_session().get_credentials()
    if c.token:
        credentials = (
            'aws_access_key_id={};aws_secret_access_key={};token={}'.
            format(c.access_key, c.secret_key, c.token))
    else:
        credentials = (
            'aws_access_key_id={};aws_secret_access_key={}'.
            format(c.access_key, c.secret_key))

    conn = psycopg2.connect(rsurl)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                '''DELETE FROM logs."{}" WHERE time between '{}' and '{}' '''.
                format(table, start, end))
            LOGGER.info('deleted %d rows from %s', cur.rowcount, table)

            cur.execute(
                '''CREATE TEMP TABLE import (LIKE logs."{}")'''.format(table))
            if not s3_dir_exists('{}/{}/'.format(run_tag, table)):
                LOGGER.error("No S3 files in {}/{}/".format(run_tag, table))
                return
            LOGGER.info('Loading into %s', table)
            cur.execute('''COPY import
                        FROM 's3://{bucket}/{run_tag}/{table}/'
                        CREDENTIALS'{credentials}'
                        {copy_opts}'''
                        .format(bucket=os.environ['COMPACTER_BUCKET'],
                                run_tag=run_tag, table=table,
                                credentials=credentials,
                                copy_opts=COPY_OPTS))

            cur.execute(('''INSERT INTO logs."{}" SELECT * FROM import ''' +
                         '''WHERE time between '{}' and '{}' ''').
                        format(table, start, end))
            LOGGER.info('inserted %d rows into %s, now committing',
                        cur.rowcount, table)
        LOGGER.info('table %s committed', table)
    except Exception:
        LOGGER.exception('Write to table %s failed', table)
    finally:
        conn.close()


def fragments(table_name):
    """Returns base64-encoded fragments of the given table_name.

    This is used to filter for input which is relevant to the given event,
    without invoking JSON unmarshalling or repeated b64 decoding, at the
    possible expense of some false positives which will be filtered out at the
    load phase anyway.
    """
    name_length = len(table_name)
    for start_idx in xrange(3):
        # end_idx = max <= name_length where 3 divides (end_idx - start_idx)
        end_idx = (name_length - start_idx) / 3 * 3 + start_idx
        yield b64encode(table_name[start_idx:end_idx])


def upload_to_db(args, start, end, run_tag, tables):
    Pool(int(args['--poolsize'])).map(
        lambda x: ingester_worker(x, start, end, args['--rsurl'], run_tag),
        tables)


def main(args):
    set_up_logging(args)

    run_tag = args.get('--from-runtag')
    processor_only = args.get('--processor-only')
    if run_tag and processor_only:
        print "Looks like you don't want to do anything; exiting"
        sys.exit(1)

    # Do our best to verify that the timestamps make sense
    start = PT.localize(
        datetime.datetime.strptime(args['START'], '%Y-%m-%d %H:%M:%S'))
    end = PT.localize(
        datetime.datetime.strptime(args['END'], '%Y-%m-%d %H:%M:%S'))

    if end <= start:
        print "Need a valid time range, got {} to {}".format(start, end)
        sys.exit(1)

    tables = None
    fragment_list = None
    if args['--all-tables']:
        tables = get_tables_from_blueprint()
    else:
        tables = args['TABLE']
        fragment_list = [f for t in tables for f in fragments(t)]

    if not run_tag:
        run_tag = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
        print "Starting processors now, dumping to runtag {}".format(run_tag)
        replay_processor(start, end, run_tag, fragment_list)

    if not processor_only:
        upload_to_db(args, start, end, run_tag, tables)


if __name__ == '__main__':
    main(docopt(__doc__))
