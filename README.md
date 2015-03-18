CASSANDRA <> ELASTICSEARCH SYNC
===============================

This is a daemon service for efficient and incremental bidirectional sync between [Cassandra](https://cassandra.apache.org) and [Elasticsearch](https://www.elastic.co).

It is implemented in Python and uses my [Cassandra Logger](http://github.com/felipead/cassandra-logger) trigger to keep track of changes in the Cassandra database, thus making it very efficient.

Synchronization is also idempotent and fault tolerant. This means that running the service with the same data more than once will produce exactly the same results.

REQUIREMENTS
------------

- Cassandra 2.1+
- Elasticsearch 1.4+
- Python 2.7

RATIONALE
---------

Traditional sync methods usually query the whole Cassandra database and then compare each row against the Elasticsearch database, which is very inefficient. Even if we restrict the query to match rows with a minimum timestamp, this is still an expensive operation due to Cassandra's distributed nature.

To solve this issue I created a custom trigger that keeps track of all commits made on any Cassandra table. Changes are recorded in a log table which is optimized to efficiently retrieve updates by timestamp order.

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

        curl -XGET 'http://localhost:9200/_all/_search' -d '{
            "query": {
                "range": {
                    "_timestamp": { "gte": "2015-03-01T00:35:00-03:00" }
                }
            }
        }'

Due to Elasticsearch's architecture and the fact that it was built with search as a first class citzen, the above query does not suffer from performance degradation. Internally, the `_timestamp` field is stored as a long integer, which is automatically indexed.

LIMITATIONS
-----------

It is not possible to sync delete updates from Elasticsearch to Cassandra. This is due to a limitation on how updates are queried on Elasticsearch.

Deletes from Cassandra to Elasticsearch, however, are fully synchronized. If you want to delete an entity, delete it from the Cassandra end and the application will automatically delete it from Elasticsearch.

REQUIREMENTS ABOUT YOUR DATA MODEL
----------------------------------

1. Your Cassandra schema must mirror your Elasticsearch mapping for any tables and document types to be synchronized. This means the following must be the same:
    - Cassandra keyspaces and Elasticsearch indexes
    - Cassandra tables and Elasticsearch document types
    - Cassandra columns and Elasticsearch fields
    - Cassandra and Elasticsearch ids

2. Every Cassandra table to be synchronized must have: 
    - A single primary key column named `id`. Composite primary keys are not supported at the moment.
    - A timestamp column with name and type `timestamp`. The timestamp must be updated whenever a row is created or updated.
    
3. All date-times must be saved in the UTC timezone.

4. All your Elasticsearch mappings must explicitly enable the `_timestamp` field. This can be done using:

        "_timestamp": {"enabled": True, "store": True}
        
    You also need to keep the `_timestamp` updated whenever a row is created or updated.

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
