import time
import socket
import functools

from clacks.core.command import command_from_callable
from clacks import ServerBase, ServerClient, ClientProxyBase, ServerCommand, BaseRequestHandler


# ----------------------------------------------------------------------------------------------------------------------
class DispatchServer(ServerBase):

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

        self.worker_pool = list()
        self._locked_workers = dict()

        self.current_client = None

        self.worker_handler = worker_handler

        # -- when the first worker registers, this list will be initialized and used as a permanent (until the next
        # -- server reboot) record of available dispatch commands.
        # -- This is used as a validation mechanism, ensuring that when server commands between different dispatch
        # -- Workers mismatch, the server will reject the new worker with mismatching commands.
        # -- This is to ensure that all dispatch workers on a given server share the exact same interface, as it would
        # -- create confusion which workers expose which commands otherwise.
        self.dispatch_commands = list()

        # -- register_worker is built in
        self.register_command(
            'register_worker', ServerCommand(
                self,
                self.register_worker,
                private=False, arg_types={'address': tuple}
            )
        )

        # -- lock_worker is built in
        self.register_command(
            'lock_worker', ServerCommand(
                self,
                self.lock_worker,
                private=False, arg_defaults={'worker': ClientProxyBase}
            )
        )

        # -- unlock_worker is built in
        self.register_command(
            'unlock_worker', ServerCommand(
                self,
                self.unlock_worker,
                private=False, arg_defaults={'worker': ClientProxyBase}
            )
        )

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
            adapter.server_pre_digest(handler, connection, transaction_id, header_data, data)

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
    def lock_worker(self) -> None:
        """
        Lock a worker to the current client. This ensures, that until the worker is unlocked, the given connection
        instance will always use the exact same worker when dispatching commands.
        """
        if self._locked_workers.get(self.current_client) is not None:
            self.logger.warning('Client %s already locked!' % str(self.current_client))
            return

        self._locked_workers[self.current_client] = self.acquire_worker()

        self.logger.warning(
            'Locked worker %s to client %s' % (self._locked_workers[self.current_client], self.current_client)
        )

    # ------------------------------------------------------------------------------------------------------------------
    def unlock_worker(self) -> None:
        """
        Unlock the current connection's worker.
        :return: None
        """
        # -- if the worker has already been unlocked (for whatever reason), do not do anything.
        if self._locked_workers.get(self.current_client) is None:
            # -- remove self.current_client from the locked workers' dictionary if the key is still there
            if self.current_client in self._locked_workers:
                del self._locked_workers[self.current_client]
            return

        # -- release the worker
        self.release_worker(self._locked_workers[self.current_client])

        # -- remove the key from the dictionary
        del self._locked_workers[self.current_client]

    # ------------------------------------------------------------------------------------------------------------------
    def release_worker(self, worker: ClientProxyBase) -> None:
        """
        Release the given worker.

        :param worker: the worker to release
        :type worker: ClientProxyBase

        :return: None
        """
        self.logger.warning('Releasing worker %s back into the pool' % worker)
        self.worker_pool.append(worker)

    # ------------------------------------------------------------------------------------------------------------------
    def _acquire_worker(self) -> ClientProxyBase | None:
        """
        Acquire a worker from the pool and lock it.

        :return: A worker proxy instance.
        :rtype: ClientProxyBase
        """
        if not len(self.worker_pool):
            return None

        # -- when a new worker is acquired, pop it from the pool; every command needs to run in a fresh worker.
        worker = self.worker_pool[0]
        self.logger.warning('Acquiring worker %s from the pool' % worker)
        self.worker_pool.pop(0)

        return worker

    # ------------------------------------------------------------------------------------------------------------------
    def acquire_worker(self) -> ClientProxyBase:
        """
        Attempt to acquire a worker. Will retry 10 times in case all of them are temporarily busy, and wait 0.5 seconds
        between attempts, waiting for a total of 5 seconds before timing out.

        :return: a DispatchServerWorker instance if one can be acquired, None otherwise.
        :rtype: ClientProxyBase
        """
        worker = None

        retries = 0
        while worker is None:
            if retries > 10:
                raise Exception('Could not acquire worker!')

            # -- attempt to acquire a worker
            worker = self._acquire_worker()

            if worker is None:
                time.sleep(0.5)

            retries += 1

        if worker is None:
            raise Exception('Could not acquire worker!')

        return worker

    # ------------------------------------------------------------------------------------------------------------------
    def worker_is_locked(self) -> bool:
        """
        Return True if the worker assigned to the current client is locked, False otherwise.

        :return: true if the current client is locked to a worker instance.
        :rtype: bool
        """
        if self._locked_workers.get(self.current_client) is not None:
            return True
        return False

    # ------------------------------------------------------------------------------------------------------------------
    def deregister_worker(self, address: tuple) -> None:
        """
        Deregister whatever worker is registered at the provided address.

        :param address: tuple of (host, port)
        :type address: tuple

        :return: None
        """
        if not isinstance(address, tuple):
            raise TypeError(f'Address must be a tuple, got {type(address)}!')

        idx = None

        for worker in self.worker_pool:
            if worker.address != address:
                continue

            idx = self.worker_pool.index(worker)
            self.logger.warning(f'De-registered worker {worker}')

        if idx is None:
            return

        self.worker_pool.pop(idx)

    # ------------------------------------------------------------------------------------------------------------------
    def register_worker(self, address: tuple) -> None:
        """
        Register a worker instance. This must be implemented by subclasses, as the proxy type, handler and marshaller
        are only known to the specific implementation.

        :param address: tuple of (host, port)
        :type address: tuple

        :return: None
        """
        if not isinstance(address, (tuple, list)):
            raise TypeError('Worker address must be a tuple or a list!')

        worker_proxy = self.construct_worker_proxy(address[0], address[1])

        if not worker_proxy:
            raise ValueError('Could not construct worker proxy at %s!' % address)

        self.logger.warning('Registering worker %s' % worker_proxy)
        self.worker_pool.append(worker_proxy)

        # -- extract commands from the worker proxy, so we know which dispatch methods are available.
        commands = worker_proxy.list_commands().response

        if commands is None:
            raise ValueError('Worker proxy %s claims it does not implement any commands' % worker_proxy)

        if not isinstance(commands, list):
            raise TypeError('Worker proxy %s did not return a command list!' % worker_proxy)

        commands = sorted(list(set([str(command) for command in commands])))

        if not self.dispatch_commands:
            self.dispatch_commands = commands

        assert commands == self.dispatch_commands, 'Worker command list did not match the template!'

        for command in commands:
            command_info = worker_proxy.command_info(command).response
            self.construct_dispatch_command(command_info, command)

    # ------------------------------------------------------------------------------------------------------------------
    def construct_dispatch_command(self, command_info: dict, command: str) -> None:
        command_key = f'dispatch_{command}'

        # -- if the command already exists, skip it.
        if command_key in self.commands:
            return

        # -- create a partial that wraps the dispatch_command redirector.
        func = functools.partial(self.dispatch_command, command)
        func.__name__ = command

        # -- decorate our server command
        for key, value in command_info.items():
            if key == 'aliases':
                value = list(['dispatch_%s' % alias for alias in value])

            # -- we want to skip arg and result processors for dispatch command proxies, as we don't want the arguments
            # -- and results to be processed both on the proxy and the server side - only the server side should ever
            # -- have a list of processors.
            # -- This layer however does allow for the opportunity to decorate dispatch proxy methods with proxy-side
            # -- processors that do things before they dispatch the command to the worker.
            if key == 'arg_processors':
                continue

            if key == 'result_processors':
                continue

            setattr(func, key, value)

        dispatch_command = command_from_callable(interface=self, function=func, cls=ServerCommand)
        self.register_command(key=command_key, _callable=dispatch_command, skip_duplicates=True)

    # ------------------------------------------------------------------------------------------------------------------
    def dispatch_command(self, key: str, *args, **kwargs) -> object:
        if self._locked_workers.get(self.current_client) is not None:
            worker = self._locked_workers[self.current_client]

        else:
            worker = self.acquire_worker()

        result = worker.question(key, *args, **kwargs)

        # -- if the worker we acquired is not locked,
        if not self.worker_is_locked():
            self.release_worker(worker)

        return result

    # ------------------------------------------------------------------------------------------------------------------
    def broadcast_command(self, key: str, *args, **kwargs) -> object:
        responses = list()

        for i in range(len(self.worker_pool)):
            worker = self.acquire_worker()

            try:
                responses.append(worker.question(key, *args, **kwargs))

            except Exception as e:
                self.release_worker(worker)
                raise e

            self.release_worker(worker)

        return responses

    # ------------------------------------------------------------------------------------------------------------------
    def get_command(self, key: str) -> ServerCommand:
        if key in ['lock_worker', 'unlock_worker']:
            return self.commands.get(key)

        # -- if we're currently locked to a worker, redirect _all_ commands to it unless they are lock / unlock
        if self.worker_is_locked():
            key = 'dispatch_%s' % key

        result = None
        if key in self.commands:
            result = self.commands.get(key)

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
