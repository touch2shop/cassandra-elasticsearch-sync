

class UpdateContainer(object):

    def __init__(self, updates):
        if updates:
            self._update_map = self.__build_update_map(updates)
            self._updates = frozenset(updates)
        else:
            self._update_map = {}
            self._updates = frozenset()

    def has_namespace(self, namespace):
        return namespace in self._update_map

    def get_namespaces(self):
        return self._update_map.keys()

    def has_table(self, namespace, table):
        if not self.has_namespace(namespace):
            return False
        return table in self._update_map[namespace]

    def get_tables(self, namespace):
        return self._update_map[namespace].keys()

    def get_updates(self, namespace, table):
        return self._update_map[namespace][table]

    def get_all_updates(self):
        return self._updates

    def __iter__(self):
        return self._updates.__iter__()

    def __len__(self):
        return len(self._updates)

    @staticmethod
    def __build_update_map(updates):
        update_map = dict()
        for update in updates:
            namespace = update.identifier.namespace
            table = update.identifier.table
            if namespace not in update_map:
                update_map[namespace] = dict()
            if table not in update_map[namespace]:
                update_map[namespace][table] = list()
            update_map[namespace][table].append(update)

        return update_map
