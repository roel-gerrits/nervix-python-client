from collections import defaultdict

from socket import AF_INET, SOCK_STREAM
from selectors import SelectorKey, EVENT_WRITE, EVENT_READ

from unittest.mock import patch as mock_patch


def patch(mock):
    return Patcher(mock.system)


class Patcher:

    def __init__(self, systemcalls):

        self.systemcalls = systemcalls

        # list of functions that should be patched
        self.patchers = [
            mock_patch('socket.socket', side_effect=self.__get_socket),
            mock_patch('selectors.DefaultSelector', side_effect=self.__get_selector),
            mock_patch('time.monotonic', side_effect=self.systemcalls.monotonic),
            # mock_patch('time.sleep', side_effect=self.systemcalls.sleep),
        ]

    def __enter__(self):
        print("=== ENTERING MOCKED ENVIRONMENT ===")
        for patch in self.patchers:
            patch.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        print("=== LEAVING MOCKED ENVIRONMENT ===")

        for patcher in self.patchers:
            patcher.stop()

    def __get_socket(self, *args, **kwargs):
        return PatchedSocket(self.systemcalls, *args, **kwargs)

    def __get_selector(self):
        return PatchedSelector(self.systemcalls)


class PatchedSocket:

    def __init__(self, systemcalls, socket_fam=None, socket_type=None, fileno=None):

        self.systemcalls = systemcalls

        if not socket_fam:
            socket_fam = AF_INET

        if not socket_type:
            socket_type = SOCK_STREAM

        if fileno:
            self._fileno = fileno
        else:
            self._fileno = self.systemcalls.socket(
                socket_fam, socket_type,
            )

    def setblocking(self, _):
        pass

    def setsockopt(self, *_):
        pass

    def bind(self, address):

        return self.systemcalls.bind(self._fileno, address)

    def listen(self, backlog=0):

        return self.systemcalls.listen(self._fileno, backlog)

    def accept(self):

        fileno, address = self.systemcalls.accept(self._fileno)

        socket = PatchedSocket(self.systemcalls, fileno=fileno)

        return socket, address

    def send(self, data):
        n = self.systemcalls.send(self._fileno, data)
        return n

    def recv(self, n):
        res = self.systemcalls.recv(self._fileno, n)
        return res

    def close(self):
        self.systemcalls.close(self._fileno)

    def fileno(self):
        return self._fileno

    def getpeername(self):
        return self.systemcalls.getpeername(self._fileno)

    def getsockname(self):
        return self.systemcalls.getsockname(self._fileno)

    def connect_ex(self, address):
        return self.systemcalls.connect(self._fileno, address)

    # def connect(self, address):
    #     self.connect_ex(address)

    def __getattr__(self, item):
        raise NotImplementedError("'{}' is not implemented".format(item))

    def __repr__(self):
        return f"<PatchedSocket fileno={self._fileno} laddr={self.getsockname()} raddr={self.getpeername()}>"


class PatchedSelector:

    def __init__(self, systemcalls):
        self.systemcalls = systemcalls

        self.keys = dict()

    def register(self, fd, events, data=None):
        fd = self.__fd(fd)

        key = SelectorKey(None, fd, events, data)

        self.keys[fd] = key
        # print(f"interest for {fd} is now {events}")

        return key

    def modify(self, fd, events, data=None):
        fd = self.__fd(fd)

        key = SelectorKey(None, fd, events, data)

        self.keys[fd] = key
        # print(f"interest for {fd} is now {events}")

        return key

    def unregister(self, fd):
        fd = self.__fd(fd)

        self.keys.pop(fd)

    def select(self, timeout=None):

        interest_table = defaultdict(set)

        for key in self.keys.values():

            if key.events & EVENT_READ:
                interest_table[key.fd].add('r')

            if key.events & EVENT_WRITE:
                interest_table[key.fd].add('w')

        event_list = self.systemcalls.select(interest_table, timeout)
        selected_keys = list()

        for fd, event_mask in event_list:
            key_events = 0
            key_events |= EVENT_READ if 'r' in event_mask else 0
            key_events |= EVENT_WRITE if 'w' in event_mask else 0

            selected_keys.append((self.keys[fd], key_events))

        return selected_keys

    def __getattr__(self, item):
        raise NotImplementedError("'{}' is not implemented".format(item))

    def __fd(self, fd_or_obj):
        if hasattr(fd_or_obj, 'fileno'):
            fileno = fd_or_obj.fileno()
        else:
            fileno = fd_or_obj

        return fileno
