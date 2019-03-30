class BaseConnection:

    def set_ready_handler(self, handler):
        raise NotImplementedError()

    def set_downstream_handler(self, handler):
        raise NotImplementedError()

    def send_verb(self, verb):
        raise NotImplementedError()
