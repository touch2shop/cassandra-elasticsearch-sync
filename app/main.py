from datetime import datetime
from time import sleep
from elasticsearch import Elasticsearch
from app.CassandraToElasticsearchPropagator import CassandraToElasticSearchPropagator
from app.cassandra_domain.store.CassandraLogEntryStore import CassandraLogEntryStore


_INTERVAL_BETWEEN_SYNCS = 10  # seconds


def run():
    cassandra_log_entry_store = CassandraLogEntryStore(["localhost"], "logger", "log")
    elasticsearch = Elasticsearch()
    cassandra_to_elastic_search = CassandraToElasticSearchPropagator(cassandra_log_entry_store, elasticsearch)

    # noinspection PyBroadException
    try:
        cassandra_to_elastic_search.propagate_updates()
        while True:
            time = datetime.utcnow()
            sleep(_INTERVAL_BETWEEN_SYNCS)
            cassandra_to_elastic_search.propagate_updates(time)

    except Exception as e:
        print e


if __name__ == "__main__":
    run()
