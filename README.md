# Spade Processor

## Overview

The Spade Processor provides a service to transform and collate stat
events into a format that is consistent with the current storage schema
for a paricular event (if it exists). If you are interested in how we
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

to the parsing pool, the parsing pool would recieve this line and extract
the data contained in the (truncated) base64 encoded json blob. This Json blob
would then be transformed into a tab delimited representation of the schema for the event as described
in the daemon's table_config.json file (an example of how that config looks is contained in the config directory).

This in turn will trigger an ingest of the data into Redshift.

## Utilities

`libexec/spade_parse --config <file>`

  runs a mini spade server that will read spade requests at localhost:8888 and output how it
  parsed them.



# License
see License

