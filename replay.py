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
             '''roundec fillrecord compupdate on emptyasnull ''' +
             '''acceptinvchars '?' trimblanks''')


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
    edge_objects =\
        boto3.resource('s3').Bucket(EDGE_BUCKET).objects
    return [s.key
            for prefix in get_days(start, end)
            for s in edge_objects.filter(Prefix=prefix)
            if s.last_modified >= start]


def contents(key):
    s3_object = boto3.resource('s3').Object(EDGE_BUCKET, key)
    gzipped_text = s3_object.get()['Body'].read()
    # " | 16" means use a gzip header
    return decompress(gzipped_text, MAX_WBITS | 16)


def pipe_through_processor(run_tag):
    def fn(key_iter):
        with open('spade-input.log', 'w') as f:
            for key in key_iter:
                f.write(contents(key))

        with open('spade-input.log', 'r') as f:
            subprocess.check_call(
                ['/opt/science/replay/bin/run_spade_replay.sh', run_tag],
                stdin=f)

    return fn


def replay_processor(start, end, run_tag):
    s3_keys = s3_object_keys(start, end)
    spark_context().\
        parallelize(s3_keys, len(s3_keys) // 20 + 1).\
        foreachPartition(pipe_through_processor(run_tag))


def get_tables_from_blueprint():
    return [row["EventName"]
            for row in requests.get(os.environ['BLUEPRINT_URL']).json()]


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
    with conn:
        with conn.cursor() as cur:
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
            LOGGER.info('inserted %d rows into %s', cur.rowcount, table)
    conn.close()
    LOGGER.info('done %s', table)


def tables_to_upload(args):
    if args['--all-tables']:
        return get_tables_from_blueprint()
    else:
        return args['TABLE']


def upload_to_db(args, start, end, run_tag):
    Pool(int(args['--poolsize'])).map(
        lambda x: ingester_worker(x, start, end, args['--rsurl'], run_tag),
        tables_to_upload(args))


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

    if not run_tag:
        run_tag = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
        print "Starting processors now, dumping to runtag {}".format(run_tag)
        replay_processor(start, end, run_tag)

    if not processor_only:
        upload_to_db(args, start, end, run_tag)


if __name__ == '__main__':
    main(docopt(__doc__))
