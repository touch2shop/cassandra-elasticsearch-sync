from datetime import datetime
from time import sleep

from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

from app.cassandra_to_elasticsearch_propagator import CassandraToElasticsearchPropagator


_INTERVAL_BETWEEN_SYNCS = 10  # seconds


def run():
    cassandra_cluster = Cluster()
    elasticsearch_client = Elasticsearch()
    cassandra_to_elastic_search = CassandraToElasticsearchPropagator(cassandra_cluster, elasticsearch_client, "logger", "log")

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
