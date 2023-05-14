import time
import clacks
import functools

from clacks import hidden
from clacks import ServerCommand
from clacks import ClientProxyBase
from clacks import ServerInterface
from clacks import command_from_callable


# ----------------------------------------------------------------------------------------------------------------------
class ClacksDispatchCoreInterface(ServerInterface):

    _PRIORITY = 99

    _REQUIRED_INTERFACES = ['cmd_utils']

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self):
        super(ClacksDispatchCoreInterface, self).__init__()
        self.worker_pool = list()
        self._locked_workers = dict()

        # -- when the first worker registers, this list will be initialized and used as a permanent (until the next
        # -- server reboot) record of available dispatch commands.
        # -- This is used as a validation mechanism, ensuring that when server commands between different dispatch
        # -- Workers mismatch, the server will reject the new worker with mismatching commands.
        # -- This is to ensure that all dispatch workers on a given server share the exact same interface, as it would
        # -- create confusion which workers expose which commands otherwise.
        self.dispatch_commands = list()

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

        worker_proxy = self.server.construct_worker_proxy(address[0], address[1])

        if not worker_proxy:
            raise ValueError('Could not construct worker proxy at %s!' % address)

        self.logger.warning('Registering worker %s' % worker_proxy)
        self.worker_pool.append(worker_proxy)

        # -- extract commands from the worker proxy, so we know which dispatch methods are available.
        response = worker_proxy.list_commands()

        commands = response.response

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

        if command_key in self.server.commands:
            return

        # -- create a partial that wraps the dispatch_command redirector.
        func = functools.partial(self.dispatch_command, command)
        func.__name__ = command

        # -- decorate our server command
        for key, value in command_info.items():
            if key == 'aliases':
                value = list(['dispatch_%s' % alias for alias in value])
            setattr(func, key, value)

        dispatch_command = command_from_callable(interface=self, function=func, cls=ServerCommand)
        self.server.register_command(key=command_key, command=dispatch_command)

    # ------------------------------------------------------------------------------------------------------------------
    def lock_worker(self) -> None:
        """
        Lock a worker to the current client. This ensures, that until the worker is unlocked, the given connection
        instance will always use the exact same worker when dispatching commands.
        """
        if self._locked_workers.get(self.server.current_client) is not None:
            self.logger.warning('Client %s already locked!' % str(self.server.current_client))
            return

        self._locked_workers[self.server.current_client] = self.acquire_worker()

        self.logger.warning(
            'Locked worker %s to client %s' % (
                self._locked_workers[self.server.current_client],
                self.server.current_client
            )
        )

    # ------------------------------------------------------------------------------------------------------------------
    def unlock_worker(self) -> None:
        """
        Unlock the current connection's worker.
        :return: None
        """
        # -- if the worker has already been unlocked (for whatever reason), do not do anything.
        if self._locked_workers.get(self.server.current_client) is None:
            # -- remove self.current_client from the locked workers' dictionary if the key is still there
            if self.server.current_client in self._locked_workers:
                del self._locked_workers[self.server.current_client]
            return

        # -- release the worker
        self.release_worker(self._locked_workers[self.server.current_client])

        # -- remove the key from the dictionary
        del self._locked_workers[self.server.current_client]

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
        if self._locked_workers.get(self.server.current_client) is not None:
            return True
        return False

    # ------------------------------------------------------------------------------------------------------------------
    @hidden
    def dispatch_command(self, key: str, *args, **kwargs) -> object:
        if self._locked_workers.get(self.server.current_client) is not None:
            worker = self._locked_workers[self.server.current_client]

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
    @hidden
    def get_command(self, key: str) -> ServerCommand:
        if key in ['lock_worker', 'unlock_worker']:
            return self.server.commands.get(key)

        # -- if we're currently locked to a worker, redirect _all_ commands to it unless they are lock / unlock
        if self.worker_is_locked():
            key = f'dispatch_{key}' if not key.startswith('dispatch_') else key
            return self.server.interfaces['cmd_utils'].get_command(key)

        try:
            return self.server.interfaces['cmd_utils'].get_command(key)
        except BaseException:
            pass

        # -- if a command is not present on the server, it should be on the slave.
        if not self.worker_is_locked():
            key = 'dispatch_%s' % key

        return self.server.interfaces['cmd_utils'].get_command(key)



clacks.register_server_interface_type('dispatch_core', ClacksDispatchCoreInterface)
