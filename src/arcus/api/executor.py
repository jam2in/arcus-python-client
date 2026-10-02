"""Resolve request destinations without depending on socket or protocol state."""


class RequestExecutor:
    """Share the client's current locator across all data-type APIs.

    Operations remain asynchronous: execute returns the exact operation produced
    by the selected node, without waiting for or transforming its result.
    """

    def __init__(self, locator):
        self.locator = locator

    def execute(self, key, operation):
        return operation(self.locator.get_node(key))

    def group_by_node(self, keys):
        """Resolve every key before dispatch while preserving key and node order."""
        groups = {}
        for key in keys:
            node = self.locator.get_node(key)
            groups.setdefault(node, []).append(key)
        return groups
