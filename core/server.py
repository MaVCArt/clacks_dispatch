import time
import socket

from clacks import ServerBase, ServerClient, ClientProxyBase, BaseRequestHandler


# ----------------------------------------------------------------------------------------------------------------------
class DispatchServer(ServerBase):

    _REQUIRED_INTERFACES = ['dispatch_core']

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(
            self,
            identifier: str,
            worker_handler: BaseRequestHandler,
            start_queue: bool = False,
            threaded_digest: bool = True
    ) -> None:
        """
        Create a dispatch server, opening a port on the given worker host at the given worker port number, for workers
        to register themselves on. This port is how workers let a server know they exist.

        :param identifier: server identifier
        :type identifier: str

        :param worker_handler: the handler instance to use for worker proxies
        :type worker_handler: BaseRequestHandler

        :param start_queue: if True, start handling requests.
        :type start_queue: bool
        """
        # -- the dispatch server has a threaded digest, which means it allows calls to it to pass through.
        # -- this ensures that the dispatch server truly serves as a "dispatch" server, not a proxy for a proxy.
        super(DispatchServer, self).__init__(
            identifier=identifier,
            start_queue=start_queue,
            threaded_digest=threaded_digest
        )

        self.current_client = None
        self.worker_handler = worker_handler

    # ------------------------------------------------------------------------------------------------------------------
    def _respond(
            self,
            handler: BaseRequestHandler,
            connection: socket.socket,
            transaction_id: str,
            header_data: dict,
            data: dict
    ) -> None:
        """
        From a given set of transaction data, respond to the provided request using the given handler and connection.
        This ensures that a request made on a connection is always responded to on that same connection, in the order
        it came into that connection, no matter how many clients are connected to the server at the same time.

        :param handler: the handler that received the transaction and that will respond to it.
        :type handler: BaseRequestHandler

        :param connection: socket object that will receive the response to this transaction.
        :type connection: socket.socket

        :param transaction_id: this transaction's ID, useful for lookup-based mechanisms.
        :type transaction_id: str

        :param header_data: the header data for the transaction
        :type header_data: dict

        :param data: the data in the transaction
        :type data: dict

        :return: None
        """
        self.current_client = self.connections[connection]

        for adapter in self.adapters.values():
            if adapter not in handler.adapters:
                continue
            adapter.server_pre_digest(self, handler, connection, transaction_id, header_data, data)

        response = self.digest(handler, connection, transaction_id, header_data, data)

        for adapter in self.adapters.values():
            if adapter not in handler.adapters:
                continue
            adapter.server_post_digest(handler, connection, transaction_id, header_data, data, response)

        handler.respond(connection, transaction_id, response)

    # ------------------------------------------------------------------------------------------------------------------
    def remove_client(self, client: ServerClient) -> bool:
        result = super(DispatchServer, self).remove_client(client)
        if client in self._locked_workers:
            self.release_worker(self._locked_workers[client])
            del self._locked_workers[client]
        return result

    # ------------------------------------------------------------------------------------------------------------------
    def start(self, blocking: bool = False) -> None:
        super(DispatchServer, self).start(blocking=False)
        if not blocking:
            return
        while not self.stopped:
            time.sleep(1.0)

    # ------------------------------------------------------------------------------------------------------------------
    def construct_worker_proxy(self, host: str, port: int) -> ClientProxyBase:
        # -- create a copy, don't reuse the same instance
        handler = self.worker_handler.__class__(self.worker_handler.marshaller.__class__())
        return ClientProxyBase((host, port), handler)
