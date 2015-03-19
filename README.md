`CASSANDRA <-> ELASTICSEARCH SYNC`
==================================

This is a daemon service for efficient and incremental bidirectional sync between [Cassandra](https://cassandra.apache.org) and [Elasticsearch](https://www.elastic.co).

It is implemented in Python and uses my [Cassandra Logger](http://github.com/felipead/cassandra-logger) trigger to keep track of changes in the Cassandra database, thus making it very efficient.

Synchronization is also idempotent and fault tolerant. This means that running the service with the same data more than once will produce exactly the same results.

REQUIREMENTS
------------

- Cassandra 2.1+
- Elasticsearch 1.4+
- Python 2.7 (*Python 3 is not supported yet because of the [time-uuid](https://pypi.python.org/pypi/time-uuid/) package*)

RATIONALE
---------

### Peformance

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

The following would be mapped to Elasticsearch as follows:

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

KNOWN ISSUES
------------

- Currently the application only access the Cassandra and Elasticsearch instances running in the `localhost`.
- Currently all indexes and all document types from Elasticsearch will be synchronized to Cassandra. A feature that would allow the user to specify which documents types should be synchronized is already planned.
- Improve exception handling. Currently, if any exception occurs, like a connection timeout, the application quits.
- The solution was not tested yet in a multi-clustered environment. Therefore, please keep in mind it is still not suitable for production.
- Although a substantial amount of unit, integration and functional tests were put in place, the code could still benefit from some additional test coverage.

USAGE
-----

You can customize the application by editing file `settings.yaml`.

The first time the application runs it does a full sync between Cassandra and Elasticsearch based. Depending on your data this might be a very time-consuming operation.

The following syncs are going to be incremental. The application stores, at all times, the current sync state at file `state.yaml`.

In case the application is interrupted for any reason, you can resume it and it will continue operation from the last state. The implemented data synchronization algorithms are idempotent, thus there is no risk on creating duplicates or corrupting data by syncing more than once.

SETUP
-----

1. Install the [Cassandra Logger](http://github.com/felipead/cassandra-logger) into your Cassandra database.

2. Create a logger trigger for every table you want to synchronize with Elasticsearch. For instance:

        CREATE TRIGGER logger ON product USING 'com.felipead.cassandra.logger.LogTrigger';

Before running the application, please start both Cassandra and Elasticsearch servers.

3. Setup Python and install dependencies through `pip`. You can use the `virtualenv` tool to create a virtual environment.

        pip install -r requirements.txt
 
4. You can start the application through the script:
 
        ./run.sh

AUTOMATED TESTS
---------------

Tests are split into black-box functional tests, which are very slow, and fast unit and integration tests.
 
To run only fast tests, use script `run-fast-tests.sh`. To run all tests, use `run-slow-tests.sh`.

After running the test suite, it might be necessary to clean-up your Cassandra database from garbage generated during the tests. You can use something like:
  
        truncate logger.log;
        
This command will delete all log entries from the keyspace `logger`.
