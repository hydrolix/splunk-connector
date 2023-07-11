# Hydrolix Splunk Integration

## Overview
This is an implementation of Splunk's [Chunked External Search Command](https://dev.splunk.com/enterprise/docs/devtools/customsearchcommands/createcustomsearchcmd/) 
integration mechanism that can run queries against Hydrolix tables. It reuses major parts of the 
[Spark connector](https://github.com/hydrolix/spark-connector) implementation for expediency reasons at the moment,
resulting in a ridiculous 236MB JAR file that bundles most of Spark... We can make it more efficient in a variety of 
ways if/when that becomes a priority.

## How to use
### Prerequisites

* Install Splunk 9.x
* [Enable Token Authentication](https://docs.splunk.com/Documentation/Splunk/9.1.0/Security/EnableTokenAuth)
* Create an auth token for whichever Splunk user you want to run Hydrolix queries as (e.g. `admin`). 
  * TODO this may not be necessary?

### Building
```
sbt assembly
```

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
```
$ cat kv.json
{
  "jdbc_url": "jdbc:clickhouse:https://gcp-prod-test.hydrolix.net:8088?ssl=true",
  "api_url": "https://gcp-prod-test.hydrolix.net/config/v1/",
  "username": "alex@hydrolix.io",
  "password": "REDACTED",
  "cloud_cred_1": "H4sIAAAREDACTED",
  "cloud_cred_2": null
}
```

#### Creating/Updating the Configuration Record
```
$ curl -k -u admin:REDACTED \
    -H "Content-Type: application/json" \
    -X POST \
    https://localhost:8089/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default \
    --data-binary @kv.json 
```

#### Checking the Configuration was Created Successfully
```shell
curl -k -u admin:REDACTED \
    https://localhost:8089/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default \
    | jq
{
  "_key": "default",
  "user": "admin",
  "jdbc_url": "jdbc:clickhouse:https://gcp-prod-test.hydrolix.net:8088?ssl=true",
  "api_url": "https://gcp-prod-test.hydrolix.net/config/v1/",
  "username": "alex@hydrolix.io",
  "password": "REDACTED",
  "cloud_cred_1": "H4sIAAAREDACTED",
  "cloud_cred_2": null
}
```

## Running Queries

Select the `hydrolix` app, make sure a narrow time range is selected in the time picker, and run an SPL search like the 
following:
```
| hdxscan table=hydro.logs fields="timestamp,level" message="Found storage." 
```

This SPL search has the following components:
* `| hdxscan` invokes the custom command rather than the built-in implicit `| search` command
* Everything else is an argument passed to the `hdxscan` command:
  * `table=hydro.logs` tells us which table to search
  * `fields="timestamp,level"` tells us which fields to return
  * `message="Found storage."` (any other name=value pair other than `table=` or `fields=`) adds an equality 
     predicate to the query. The predicate must refer to a String-typed field, or you'll get an error.

## Limitations & Future Plans
Currently, the `hdxscan` command will EITHER:
* Only run on the Splunk search head, severely limiting performance
* Run on multiple indexers, each returning the same duplicate results

This is because of Splunk's limited and idiosyncratic implementation of Map-Reduce, in which:

* in a search like `|foo |bar |baz`, where `foo` is an "eventing" command and `bar` is a "reporting" command:
  * `foo` will run in parallel on indexers
  * `bar` must run on the search head, because it needs to see the entire result set to finalize aggregations
  * `baz` must ALSO run on the search head, even if `baz` is also an "eventing" command.

We would prefer to run searches like this:
` | hdxplan table=hydro.logs shardKey="abc123" | hdxscan fields="timestamp,level"`, in which:
1. the `hdxplan` command runs once, and:
    * loads configuration 
    * enumerates the partitions that should be scanned
    * emits partition metadata "events" describing which partitions need to be scanned
    * doesn't read any actual data
2. the `hdxscan` command runs in parallel, and:
    * consumes the partition metadata "events" that were produced by `hdxplan`
    * executes `turbine_cmd hdx_reader ...` commands to read the actual data
    * emits the actual data as CSV

If our suspicions are correct, and Splunk can't support this one-to-many style orchestration, we have an idea for how 
multiple `hdxscan` instances running in parallel could coordinate their workload and avoid producing duplicates, using 
Zookeeper or equivalent system that supports distributed atomic writes:
* on startup, every instance attempts to atomically create a root node for the currently executing search
* only one of the instances will succeed, all the rest switch to polling until the planning is done
* on the instance that succeeds in creating the root node:
  * read configuration, enumerate partitions, do partition pruning
  * write a record in zookeeper for each partition that needs to be scanned
  * unlock the plan and transition to scan mode
* on every instance, including the one that was temporarily the planner:
  * 