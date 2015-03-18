from multiprocessing import Process
from uuid import uuid4

import pytest

from app import application
from app.elasticsearch_domain.store.abstract_elasticsearch_store import MATCH_ALL_QUERY


def random_filename(extension):
    return unicode(uuid4()) + extension


def wipe_databases(cassandra_log_entry_store, elasticsearch_client):
    cassandra_log_entry_store.delete_all()
    elasticsearch_client.delete_by_query(index="_all", body=MATCH_ALL_QUERY)


@pytest.fixture(scope="function")
def state_filename(tmpdir):
    return unicode(tmpdir.join(random_filename(".yaml")))


@pytest.fixture(scope="session")
def sync_interval_time(settings):
    return settings.interval_between_runs * 5


# noinspection PyShadowingNames
@pytest.fixture(scope="function", autouse=True)
def setup(request, settings, state_filename, cassandra_log_entry_store, elasticsearch_client):
    wipe_databases(cassandra_log_entry_store, elasticsearch_client)

    def run_sync_loop():
        application.run(settings, state_filename)

    sync_process = Process(target=run_sync_loop)
    sync_process.start()

    def on_teardown():
        if sync_process.is_alive():
            sync_process.terminate()

    request.addfinalizer(on_teardown)
