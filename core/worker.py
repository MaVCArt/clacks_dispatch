"""
Dispatch Server Worker
======================

This base class is used for dispatch server workers, and contain default behaviour to register themselves on their
parent server.

The Dispatch Server Worker is a very minimal class; almost every implementation type of this kind of server needs
most of its methods reimplementing, so it is kept bare-bones on purpose.
"""
import time

from clacks.core.server import ServerBase
from clacks.core.proxy import ClientProxyBase
from clacks.core.handler import BaseRequestHandler


# ----------------------------------------------------------------------------------------------------------------------
class DispatchServerWorker(ServerBase):

    _REQUIRED_INTERFACES = ['standard']

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, identifier, handler, address, parent_proxy):
        # type: (str, BaseRequestHandler, tuple, ClientProxyBase) -> None
        super(DispatchServerWorker, self).__init__(identifier=identifier, start_queue=False)

        # -- instance the parent proxy connection, so there is two-way traffic
        self.parent = parent_proxy

        self.handler = handler
        self.handler.socket_is_blocking = True

        self.host, self.port = address

        # -- dispatch server workers only have one handler, as they only need to listen to incoming traffic from their
        # -- dispatch parent.
        self.register_handler(self.host, self.port, self.handler)

    # ------------------------------------------------------------------------------------------------------------------
    def start(self, blocking=False):
        super(DispatchServerWorker, self).start(blocking=False)

        # -- register this worker with the parent server
        self.parent.register_worker((self.host, self.port))

        if blocking:
            while not self.stopped:
                time.sleep(1.0)
