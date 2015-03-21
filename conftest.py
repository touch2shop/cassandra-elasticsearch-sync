# py.test configuration file.

# This variable tells py.test which files and folders to ignore.
# Ignoring "apache-cassandra*" because of Travis-CI.
collect_ignore = ["env", "app", "apache-cassandra*"]
