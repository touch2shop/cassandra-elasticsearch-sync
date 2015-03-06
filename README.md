cassandra <> elasticsearch sync
===============================

**WORK IN PROGRESS**

Daemon job to automatically sync data between Cassandra and elasticsearch. 

Synchronization is done in a bidirectional way.

ASSUMPTIONS ABOUT YOUR DATA MODEL
---------------------------------

- Cassandra schema must mirror the Elasticsearch mapping of all tables and document types to be synchronized. This means the following must be the same:
    - Cassandra keyspaces and Elasticsearch indexes
    - Cassandra tables and Elasticsearch document types
    - Cassandra columns and Elasticsearch fields
    - Cassandra and Elasticsearch ids

- The names of all keyspaces, indexes, tables, document types, columns and fields must be lower case. Underscores are supported.

- Every Cassandra table to be synchronized must have: 
    - A single primary key column named `id`.
    - A timestamp column with name and type `timestamp`. The timestamp must be updated whenever a row is updated.

- All date and time columns must be saved in UTC time.
