# Hydrolix Splunk Integration

## Overview
This is an implementation of Splunk's [Chunked External Search Command](https://dev.splunk.com/enterprise/docs/devtools/customsearchcommands/createcustomsearchcmd/) 
integration mechanism that gives Hydrolix customers the ability to run SPL queries directly against Hydrolix clusters, 
without ETL. 

It's based on the [connectors-core](https://github.com/hydrolix/connectors-core/) library, like the 
[Spark](https://github.com/hydrolix/spark-connector/) and [Trino](https://github.com/hydrolix/trino-connector/) 
connectors, and shares a similar feature set.

## How to use
### Prerequisites

* Install Splunk 9.x
* [Enable Token Authentication](https://docs.splunk.com/Documentation/Splunk/9.1.0/Security/EnableTokenAuth)
* Create an auth token for whichever Splunk user you want to run Hydrolix queries as (e.g. `admin`) and save the token
  value somewhere.
* A [Zookeeper](https://zookeeper.apache.org/) cluster that's reachable from the Search Head and any indexers/peers you want to use this connector 
  with. Zookeeper is used to coordinate the distribution of work between search head and search peers. 

### Building
Install [SBT](https://scala-sbt.org/) in whatever way makes sense for your OS

```
cd ~/dev
git clone git@github.com:hydrolix/splunk-connector.git hydrolix-splunk-connector
cd hydrolix-splunk-connector
sbt -J-Xmx4G assembly
```

If all goes well this will produce `~/dev/hydrolix-splunk-connector/target/scala-2.13/splunk-interop-assembly-0.2.0-SNAPSHOT.jar`,
which is referenced in [commands.conf](./app/default/commands.conf) with a hardcoded path; you'll need to update it
to suit your environment, because Splunk doesn't support environment variables in .conf files! :/ 

### Installation
#### Running in-place
* Create a symlink from [app](./app) to `$SPLUNK_HOME/etc/apps/hydrolix/` (`$SPLUNK_HOME` is `/opt/splunk` on Linux, but
different on Mac/Windows)
* Edit [commands.conf](./app/default/commands.conf) to set the jar filenames

#### Building and Deploying
TODO there's no process for this yet!

### Configuration
Currently, we store configuration in a single record named `default` in the Splunk KVstore (a bundled, white-labeled 
MongoDB). If you deploy this as a Splunk app (e.g. untar it in `$SPLUNK_HOME/etc/apps/hydrolix`) the existing 
[collections.conf](app/default/collections.conf) file will take care of creating the "table" but you'll still need to 
create a configuration record: 

#### Configuration Record JSON
```shell
$ cat kv.json
```
```json
{
  "_key": "default",
  "jdbc_url": "jdbc:clickhouse:tcp://hydrolix.example.com:8088/_local?ssl=true",
  "api_url": "https://hydrolix.example.com/config/v1/",
  "username": "user@example.com",
  "password": "REDACTED",
  "cloud_cred_1": "H4sIAAAREDACTED",
  "cloud_cred_2": null,
  "zookeeper_servers": ["localhost:2181"]    
}
```

#### Creating/Updating the Configuration Record
```shell
$ curl -k -u admin:REDACTED \
    -H "Content-Type: application/json" \
    -X POST \
    https://localhost:8089/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default \
    --data-binary @kv.json 
```

#### Checking that the Configuration was Created Successfully
```shell
curl -k -u admin:REDACTED \
    https://localhost:8089/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default \
    | jq
```
```json
{
  "_key": "default",
  "_user": "admin",
  "jdbc_url": "jdbc:clickhouse:https://hydrolix.example.com:8088?ssl=true",
  "api_url": "https://hydrolix.example.com/config/v1/",
  "username": "user@example.com",
  "password": "REDACTED",
  "cloud_cred_1": "H4sIAAAREDACTED",
  "cloud_cred_2": null,
  "zookeeper_servers": ["localhost:2181"]  
}
```

#### Giving Search Peers Access to KVStore
Any indexer or search peer you want to participate in a Hydrolix distributed search needs access to the KVStore on the 
Search Head, and isn't given such access automatically. Create a `local/commands.conf` file on the peers/indexers with 
contents like the following:
```
command.arg.4 = https://<KV store hostname>:8089/servicesNS/nobody/hydrolix/storage/collections/data/config
command.arg.5 = <splunk admin username> or "Bearer"
command.arg.6 = <splunk admin password> or <splunk authn token value>
```
Note that the indices used here (4, 5, 6) must follow contiguously from those used in 
[default/commands.conf](./app/default/commands.conf): if you add or remove any `command.arg.<n>` lines in there, 
you'll need to renumber the `command.arg.<n>` lines in the `local/commands.conf` on your peers/indexers to suit.

## Running Queries

Select the `hydrolix` app, make sure a narrow time range is selected in the time picker, and run an SPL search like the 
following:
```
| hdxquery table=hydro.logs fields="timestamp,level" message="Found storage." 
```

This SPL search has the following components:
* `| hdxquery` invokes the custom command rather than the built-in implicit `| search` command
* Everything else is an argument passed to the `hdxquery` command:
  * `table=hydro.logs` tells us which table to search
  * `fields="timestamp,level"` tells us which fields to return
  * `message="Found storage."` (any other name=value pair other than `table=` or `fields=`) adds an equality 
     predicate to the query. The predicate must refer to a String-typed field, or you'll get an error.

# How it Works

## INIT phase
ALL nodes participating in the search (a search head, plus zero or more indexers where the app/command has been 
deployed and are registered as remote peers with the search head) do the following:
 * Generate a unique UUID. This ID is only used for the duration of this search.
 * Read the [HDXConfig](src/main/scala/io/hydrolix/splunk/model.scala#L77-101) from the Splunk KV store
   * On the search head, we can use the [URL and session key provided in the command's `getinfo` request](./src/main/scala/io/hydrolix/splunk/model.scala#L34-35).
   * On indexers, we need command-line arguments to give us the URL and authentication token of the KV store where
     configuration will be made available (typically on the search head)
 * Canonicalize the Splunk Search ID (`sid`):
   * On the search head, it will be a fractional timestamp, e.g. `1234567890.12345` and can be used as-is
   * On indexers, it will be of the form `remote_<host.name>_<timestamp>`, so we need to strip out the prefix and leave
     only the fractional timestamp.
 * Unconditionally sleep 2 seconds to make sure all workers are likely to have had enough time to connect to Zookeeper
 * Retry 10 times, twice per second to connect to Zookeeper
 * Initiate a leader election process incorporating the canonical `sid` in the path 
 * Retry 5 times once per second until the leadership election has concluded
 * If leadership election has not concluded, crash
 * Once leadership election has concluded, the leader assumes the PLANNER role, and all non-leaders assume the WORKER 
   role.
 * Proceed to the PLAN phase.

## PLAN phase

### PLANNER role
During the PLAN phase, the singular node that "won" the leader election assumes the PLANNER role and does the following:
 * Connect to the Hydrolix catalog (via API & JDBC) to identify which partitions will need to be scanned for this query
   * This does the following optimizations:
     * partition elimination by time range using the Splunk search time picker
     * projection elimination (only retrieving columns that are requested/referenced) 
     * (limited) predicate pushdown (strings only, equality only)
 * Get the IDs of all nodes participating in this search (including the planner's ID) from the Zookeeper leader election
 * Write the [QueryPlan](src/main/scala/io/hydrolix/splunk/model.scala#L98-135) to the Splunk KV store, including
   the list of participating node IDs
 * Distribute every partition that needs to be scanned to one of the workers that was identified in the leader election
 * For each worker, write a [ScanJob](src/main/scala/io/hydrolix/splunk/model.scala#L137-145) to the Splunk KV store
 * Remember the `QueryPlan` and (self-assigned) `ScanJob` in local variables
 * Transition to the SCAN phase

### WORKER role
During the PLAN phase, every node that lost the leader election assumes the WORKER role and does the following:
 * Retry 30 times, once per second, to read the QueryPlan for the current `sid` from the Splunk KV store; if the plan
   can't be read during this time, crash
 * Check whether this worker's ID is included in the plan's `worker_ids` field. If not, exit quietly without doing 
   anymore work--this node probably joined the leader election too late for its ID to be seen by the planner.  
 * Retry 10 times, once per second, to read a scan job for the current `sid` and the current worker ID from the 
   Splunk KV store; if the scan job can't be read during this time, crash 
 * Once the QueryPlan and ScanJob have been read from the KV store, transition to the SCAN phase

## SCAN phase
On every node participating in the search, including the node that _was_ the PLANNER:
 * Create a temporary CSV file
 * For each partition path in this node's assigned `ScanJob`:
   * Use the `QueryPlan` and partition path to instantiate a `HdxPartitionReader`
   * For record read from the partition:
     * Evaluate time range and string equality predicates
       * Note that we need to eval these predicates since `turbine_cmd hdx_reader` only does block-level filtering 
         at best.
     * Write records that satisfy predicates to the temporary CSV file
 * Stream the temporary CSV file to stdout, indicating there won't be any more data from this worker by setting 
   `finished=true` in the response metadata.

# Roadmap Items

### Secret Management
Stop storing the cleartext Hydrolix password in the KVstore. Splunk has a [secret service](https://dev.splunk.com/enterprise/docs/developapps/manageknowledge/secretstorage/), 
but it's not clear how to access it from an external command.

### Non-ZK operating mode
Consider implementing an operating mode that still works when there's no Zookeeper, e.g. by disclaiming the ability
to run in parallel

### Store query plans & scan jobs in ZK
Consider storing plans and scan jobs in ZK instead of KVstore, so they can be garbage-collected automatically. Before 
doing this, check if object size in ZK is likely to be an issue, because plans can be large (e.g. `select *` needs the
entire schema, which could be 500kB)

## Longer Term

### Descending Time
Splunk normally wants searches to return later results first; consider whether we want to try to emulate this. I don't 
think there's a correctness issue here, just UX. 

### Preserve order of Hydrolix results
Currently, all workers are scanning partitions and sending output simultaneously, so when there's more than one worker, 
it's extremely unlikely that the order of data as retrieved in the Hydrolix table will be preserved. This won't matter 
for some use cases (e.g. `|stats` or `|timechart`), but will not yield expected results for other use cases that are 
sensitive to the order of events (e.g. `|transaction`, which does sessionization into contiguous time windows)
