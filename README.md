`CASSANDRA <-> ELASTICSEARCH sync`
==================================

This is a daemon service for efficient and incremental bidirectional sync between [Cassandra](https://cassandra.apache.org) and [Elasticsearch](https://www.elastic.co).

It is implemented in Python and uses my [Cassandra Logger](http://github.com/felipead/cassandra-logger) trigger to keep track of changes in the Cassandra database, thus making it very efficient.

Synchronization is also idempotent and fault tolerant. This means that running the service with the same data more than once will produce exactly the same results.

REQUIREMENTS
------------

- Cassandra 2.1+
- Elasticsearch 1.4+
- Python 2.7+ (*Python 3 is not supported yet because of the [time-uuid](https://pypi.python.org/pypi/time-uuid/) package*)

RATIONALE
---------

### Performance

Traditional sync methods usually query the whole Cassandra database and then compare each row against the Elasticsearch database, which is very inefficient. Even if we restrict the query to match rows with a minimum timestamp, this is still an expensive operation due to Cassandra's distributed nature.

To solve this issue I created a custom trigger that automatically tracks all commits made on any Cassandra table that has the trigger enabled. Changes are recorded in a log table which is optimized to efficiently retrieve updates by timestamp order.

This is the schema of the log table:

        CREATE TABLE log (
            time_uuid timeuuid,
            logged_keyspace text,
            logged_table text,
            logged_key text,
            operation text,
            updated_columns set<text>,
            PRIMARY KEY ((logged_keyspace, logged_table, logged_key), time_uuid)
        ) WITH CLUSTERING ORDER BY (time_uuid DESC);

which can be queried efficiently on Cassandra by:

        SELECT * FROM log WHERE time_uuid >= minTimeuuid('2015-03-01 00:35:00-0300') ALLOW FILTERING;

From the Elasticsearch end we can make a similar query by running:

        $ curl -XGET 'http://localhost:9200/_all/_search' -d '{
            "query": {
                "range": {
                    "_timestamp": { "gte": "2015-03-01T00:35:00-03:00" }
                }
            }
        }'

Due to Elasticsearch's architecture and the fact that it was built with search as a first class citzen, the above query does not suffer from performance degradation. Internally, the `_timestamp` field is stored as a long integer, which is automatically indexed.

### Breaking Cycles

One of the common problems faced in bidirectional syncing is how to avoid cycles. For instance, applying updates from Cassandra to Elasticsearch could generate another set of updates from Elasticsearch to Cassandra, which in turn would generate more update from Cassandra to Elasticsearch, and so on... One could end up creating an infinite update cycle if he is not careful. Here is an example diagram:

        CASSANDRA                                      ELASTICSEARCH
        
        {id: 1, name: "alice", timestamp: 1}   --->   {id: 1, name: "bob", timestamp: 2}
        {id: 1, name: "alice", timestamp: 3}   <---   {id: 1, name: "alice", timestamp: 2}
        {id: 1, name: "alice", timestamp: 3}   --->   {id: 1, name: "alice", timestamp: 4}
        {id: 1, name: "alice", timestamp: 5}   <---   {id: 1, name: "alice", timestamp: 4}

...and so on.

There are several techniques to break such cycles. One that is simple and also very effective is to only apply updates from one database to another if data is different. It is easy to see how this breaks the cycle in two iterations:

        CASSANDRA                                      ELASTICSEARCH
        
        {id: 1, name: "alice", timestamp: 1}   ---->   {id: 1, name: "bob", timestamp: 2}
        {id: 1, name: "alice", timestamp: 1}           {id: 1, name: "alice", timestamp: 2}

### Versioning and Conflict Resolution

This sync algorithm uses *timestamps* for versioning. As long as timestamps are constantly updated whenever a table or document is updated, we can assume the data that contains the most recent timestamp is also the most recent.

For the unlikely scenario where two entities are updated on both databases at the exact same moment with millisecond precision, a conflict would be generated. It would be impossible to know which version to use. 

Solving this problem is not a trivial task and I chose to ignore it for now. In the event of such conflict, the service will leave each row as it is.

LIMITATIONS
-----------

It is not possible to sync delete updates from Elasticsearch to Cassandra. This is due to a limitation on how updates are queried on Elasticsearch.

Deletes from Cassandra to Elasticsearch, however, are fully synchronized. If you want to delete an entity, delete it from the Cassandra end and the application will automatically delete it from Elasticsearch.

DATA MODELLING
--------------

### Requirements

1. Your Cassandra schema must mirror your Elasticsearch mapping for any tables and document types to be synchronized. This means the following must be the same:
    - Cassandra keyspace names and Elasticsearch index names
    - Cassandra table names and Elasticsearch document type names
    - Cassandra column names and Elasticsearch field names
    - Cassandra and Elasticsearch ids

2. Every Cassandra table to be synchronized must have: 
    - A single primary key column named `id`. Composite primary keys are not supported at the moment.
    - A timestamp column with name and type `timestamp`.
 
3. All `ids` must be unique. Supported types are: [UUID](http://en.wikipedia.org/wiki/Universally_unique_identifier), integer and string.

4. Only simple column types are supported at the moment:
  
    - integer / long
    - float / double
    - decimal (must be mapped as string in Elasticsearch due to lack of support).
    - boolean
    - string / text
    - date / timestamp
    - UUID / Time UUID (must be mapped as string in Elasticsearch due to lack of support).

  Other types could work in theory, but were not tested yet.

5. Date-times must be saved in UTC timezone.

6. All your Elasticsearch mappings must explicitly enable the `_timestamp` field. This can be done using:

        "_timestamp": {"enabled": True, "store": True}

8. Your application code must update the *timestamp* fields whenever an entity is created or updated. The sync service requires all tables and doc types to have a timestamp, otherwise it will fail.

### Example Data Model

Here's an example Cassandra schema:

        CREATE TABLE product (
            id uuid PRIMARY KEY,
            name text,
            quantity int,
            description text,
            price decimal,
            enabled boolean,
            external_id uuid,
            publish_date timestamp,
            timestamp timestamp
        )

This schema would be mapped to Elasticsearch as follows:

        $ curl -XPUT "http://localhost:9200/example/product/_mapping" -d '{
            "product": { 
                "_timestamp": { "enabled": true, "store": true },
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "quantity": {"type": "integer"},
                    "price": {"type": "string"},
                    "enabled": {"type": "boolean"},
                    "publish_date": {"type": "date"},
                    "external_id": {"type": "string"}
                }
            }
        }'
        
Notice that the UUID and decimal types on Cassandra are mapped on Elasticsearch as string. This is because Elasticsearch does not support these types natively. 

Please remember that, in general, decimal numbers should not be stored as floating point numbers. The precision loss induced by floating point arithmetic could cause a significant impact on financial and business applications. You can read more about it [here](https://docs.python.org/3/library/decimal.html).

APPLICATION
-----------

### Usage

At the first run the service performs a full sync between Cassandra and Elasticsearch. Depending on the volume of your data this might be a very time-consuming operation.

The following syncs are going to be incremental and much faster. The application automatically stores, at all times, the current sync state in a file called `state.yaml`. In case the service is interrupted for any reason, you can restart it and it will continue operation from the last saved state. The implemented data synchronization algorithms are idempotent, thus there is no risk on creating duplicates or corrupting the database by syncing data more than once.

### Configuration

By default the service tries to connect to Cassandra and Elasticsearch at the `localhost` using the respective standard ports and without authentication.

This can be changed by defining the following environment variables:

- `ELASTICSEARCH_URLS`: A list of connection URLs for each Elasticsearch server, separated by space. For instance: `"https://felipe:345%340@us-east-1.bonsai.io"`. If not defined, connects to localhost.  
- `CASSANDRA_USERNAME`: The Cassandra username. Leave empty for no authentication.
- `CASSANDRA_PASSWORD`: The Cassandra password. Leave empty for no password.
- `CASSANDRA_PORT`: The Cassandra port. If not defined, uses default.
- `CASSANDRA_NODES`: A list of cassandra node ips, separated by space. For instance: `"192.168.0.100 192.168.0.3"`. If not defined, connects to localhost. 

You can customize other parameters by editing file `settings.yaml`.

### Setup

1. Install the [Cassandra Logger](http://github.com/felipead/cassandra-logger) into every node of your Cassandra cluster.

2. Create the Cassandra Logger [schema](https://github.com/felipead/cassandra-logger/blob/master/create-log-schema.cql).

3. For every Cassandra table that you want to synchronize, you need to create a logger trigger:

        CREATE TRIGGER logger ON product USING 'com.felipead.cassandra.logger.LogTrigger';

4. Setup Python and install dependencies through `pip`. You might want to use the `virtualenv` tool to create a virtual environment first.

        pip install -r requirements.txt

### Running
 
Start Cassandra and Elasticsearch, if they are not already running.

Run the service through the script (it will run in foreground):
 
    ./run.sh

KNOWN ISSUES
------------

- You can control wich Cassandra tables you want to be synchronized by creating or dropping the logger trigger on each of them. However, the same is not possible with Elasticsearch. The service will read all indexes and all document types from Elasticsearch and try to sync them with Cassandra. This will fail if Cassandra does not have all the corresponding tables. However, a feature that would allow the user to specify in `settings.yaml` which indexes and keyspaces it wants to be synchronized is already planned.

- Improve exception handling. Currently, if any exception occurs, like a connection timeout, the application quits. This is a deal breaker if the application needs to run as a daemon.

- The solution was not tested yet in a multi-clustered environment. Therefore, please keep in mind it is still not suitable for production.

AUTOMATED TESTS
---------------

First, install test dependencies through pip:

    pip install -r requirements.test.txt

Tests are split into black-box functional tests, which are very slow, and fast unit and integration tests.
 
To run only fast tests, use:
 
        ./test-fast.sh
        
To run all tests, use:

        ./test-slow.sh

You need to have both Cassandra and Elasticsearch running before running the tests.