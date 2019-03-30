from collections import deque

from nervix.protocols.base import BaseConnection


class MockedConnection(BaseConnection):

    def __init__(self):
        self.ready = False
        self.ready_handler = None
        self.downstream_handler = None

        self.upstream_verbs = deque()

    def set_ready_handler(self, handler):
        self.ready_handler = handler

    def set_downstream_handler(self, handler):
        self.downstream_handler = handler

    def send_verb(self, verb):
        self.upstream_verbs.append(verb)

    def mock_connection_ready(self, ready):
        self.ready = ready
        if self.ready_handler:
            self.ready_handler(self.ready)

    def mock_downstream_verb(self, verb):
        self.downstream_handler(verb)

    def assert_upstream_verb(self, verify_verb):

        if not self.upstream_verbs:
            if verify_verb is None:
                return

            raise AssertionError(f"Expected {verify_verb}, but got nothing")

        actual_verb = self.upstream_verbs.popleft()

        if verify_verb is None:
            raise AssertionError(f"Expected nothing, but got {actual_verb}")

        if actual_verb != verify_verb:
            raise AssertionError(f"Expected {verify_verb} but got {actual_verb}")
