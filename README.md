CASSANDRA <> ELASTICSEARCH SYNC (efficiently)
=============================================

This is a daemon job to efficiently and incrementally sync data from Cassandra to Elasticsearch and vice-versa. It is implemented in Python.

It uses my [Cassandra Logger](http://github.com/felipead/cassandra-logger) trigger to keep track of changes in the Cassandra database, which is very efficient.

Synchronization is also idempotent and fault tolerant. This means that running the synchronization for the same data more than once will produce exactly the same results.  

RATIONALE
---------

Traditional sync methods usually query the whole Cassandra database and then compare each row against the Elasticsearch database, which is very inefficient. Even if we restrict the query to match rows with a minimum timestamp, this is still an expensive operation due to Cassandra's distributed nature.

To solve this issue I created a custom trigger that keeps track of all commits made on any Cassandra table. Changes are recorded in a log table which is optimized to efficiently retrieve updates by timestamp order.

REQUIREMENTS
------------

- Cassandra 2.1+ with [Cassandra Logger](http://github.com/felipead/cassandra-logger) trigger
- Elasticsearch 1.4+
- Python 2.7

LIMITATIONS
-----------

It is currently not possible to sync delete updates from Elasticsearch to Cassandra. This is due to a limitation on how updates are queried on Elasticsearch.

Deletes from Cassandra to Elasticsearch, however, are fully synchronized. If you want to delete an entity, delete it from the Cassandra end and the application will automatically delete it from Elasticsearch.

REQUIREMENTS ABOUT YOUR MODEL
-----------------------------

1. Your Cassandra schema must mirror your Elasticsearch mapping for any tables and document types to be synchronized. This means the following must be the same:
    - Cassandra keyspaces and Elasticsearch indexes
    - Cassandra tables and Elasticsearch document types
    - Cassandra columns and Elasticsearch fields
    - Cassandra and Elasticsearch ids

2. Every Cassandra table to be synchronized must have: 
    - A single primary key column named `id`. Composite primary keys are not supported at the moment.
    - A timestamp column with name and type `timestamp`. The timestamp must be updated whenever a row is created or updated.
    
3. All date-times must be store in the UTC timezone.

4. All your Elasticsearch mappings must explicitly enable the `_timestamp` field. This can be done using:

        "_timestamp": {"enabled": True, "store": True}
        
    You also need to keep the `_timestamp` updated whenever a row is created or updated.

KNOWN ISSUES
------------

- Currently the application only access the Cassandra and Elasticsearch instances running in the localhost.
- Currently all indexes and all document types from Elasticsearch will be synchronized to Cassandra. A feature that would allow the user to specify which documents types should be synchronized is already planned.
- Improve exception handling. Currently, if any exception occurs, like a connection timeout, the application quits.
- The solution was not tested yet in a multi-clustered environment. Therefore, please keep in mind it is still not suitable for production.
- Although a substantial amount of unit, integration and functional tests were put in place, the code could still benefit from some additional test coverage.

USAGE
-----

You can customize the application by editing file `settings.yaml`.

The first time the application is run it does a full sync between Cassandra and Elasticsearch based on the logs. This might be a time-consuming operation.

The following syncs are going to be incremental. The application stores, at all times, the current sync state at file `state.yaml`.

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
