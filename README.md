***NOTE: This project is no longer being updated publicly.***

# Spade Processor

## Overview

The Spade Processor provides a service to transform and collate stat
events into a format that is consistent with the current storage schema
for a particular event (if it exists). If you are interested in how we
create and maintain schemas take a look at twitchscience/blueprint.

The above has a lot of big words that are kind of confusing so lets start off
with an example.

## What the Processor Does

The processor can be described by the following diagram:
```
Downloads logs from S3
   +–+
   |
+––+––+     +–––––––––––––––––––––+––––+–––+
|     |     |                     |    |   ++
|     |     |                     |    +––––+       XXXXXXXXX
|     |     |                     |    |   ++  XXXXXX       XX
|     |     | Parsing Pool        |    +––––+ XX            XX
|     |     |                     |    |   ++  X            XX
|     +–––––+                     |    +––––––––+X   S3      X
|     |     |                     |    |   ++    XX           X
|     |  +  |                     |    +––––+  XX          XXXX
+–––––+  |  +–––––––––––––––––––––+––––+   ++  XXXXXXXXXXXXX
         |
         |                        +––––––––+
         | chan []byte            Writer Controller and Writers
         +
```

By this model if we attached a thread that simple output

```
222.22.222.222 [1395707641.000] data=eyJwcm9wZXJ0a...GNoZWQifQ== 09fff3e1-49eff880-535707f3-20f9e114d784a3fa
```

to the parsing pool, the parsing pool would receive this line and extract
the data contained in the (truncated) base64 encoded json blob. This Json blob
would then be transformed into a tab delimited representation of the schema for the event as described
in the daemon's table_config.json file (an example of how that config looks is contained in the config directory).

This in turn will trigger an ingest of the data into Redshift.

## Testing

If you are on a mac, to run the tests you need to brew install `pkg-config` and `gzrt`.  If you are running this
on a mac with Xcode 8.3 and Go < 1.8.1, then you need to provide `-ldflags -s` to your run.

## Replay mode

It is also possible to replay data from an S3 bucket of deglobbed inputs
which failed to process properly for some reason (errors in the Blueprint schema or
bugs in Spade, for example).  Once this is done, the replay script will automatically
reingest the reprocessed data into Redshift, replacing the erroneous records if any.
Replay can also publish to Kinesis streams, but consumers must be aware that replays
are possible.

To run in replay mode:

1. Deploy a Spark cluster where each worker node has Spade installed.  Spade should be
isolated by only allowing one task at a time on each worker node.
2. Inside the Spade directory, run
```
./replay.sh MASTER_IP REDSHIFT_TARGET START END [TABLE ... | --all-tables]
```
where
	* `MASTER_IP` is the central node of the Spark cluster;
	* `REDSHIFT_TARGET` is the destination Redshift cluster;
	* `START` and `END` specify the time window to be replaced, in Pacific time in
	  the format `%Y-%m-%d %H:%M:%S`;
	* `TABLE ...` specifies the tables whose records should be replaced.
3. If the results of the previous step are unsatisfactory, make necessary
   changes and repeat.

Replay is a 2 step process - transforming edge logs on Spark, and loading the transformed
data into Redshift. For flexibility, there are 2 flags that lets you skip either steps.
`--skip-ace-upload` will have replay do the edge log transformation, and will place the
transformed files in S3, but will stop short of actually uploading into Redshift.
`--skip-transform` will have replay skip the edge log transformation step, and directly load
a runtag's transformed data into Redshift - if this flag is specified, `--runtag` must
also be specified. For example, `--skip-ace-upload` is used by Tahoe replay because the
transformed files are Tahoe-bound instead of Ace-bound.

For extra reliability, instead of running `replay.sh` locally, start a tmux session on the
master node and submit from there.

We expect this tool to be used only every few months; if your needs are
similar and the Spark cluster you bring up is only for this tool, consider
tearing it down.

## Utilities

`libexec/spade_parse --config <file>`

  runs a mini spade server that will read spade requests at localhost:8888 and output how it
  parsed them.

## License
see License
