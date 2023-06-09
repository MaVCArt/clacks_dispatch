import time
import uuid
import clacks
import unittest
import threading
import clacks_dispatch


# ----------------------------------------------------------------------------------------------------------------------
class DispatchWorkerTestInterface(clacks.ServerInterface):

    # ------------------------------------------------------------------------------------------------------------------
    def message(self, msg):
        self.server.messages.append(msg)

    # ------------------------------------------------------------------------------------------------------------------
    def foo(self):
        return self.server.identifier

    # ------------------------------------------------------------------------------------------------------------------
    def echo(self, arg):
        return arg


# ----------------------------------------------------------------------------------------------------------------------
class DispatchWorkerTest(clacks_dispatch.DispatchServerWorker):

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, identifier, handler, address, parent_proxy):
        super(DispatchWorkerTest, self).__init__(identifier, handler, address, parent_proxy)

        interface = DispatchWorkerTestInterface()
        self.register_interface('test', interface)

        self.messages = list()


class TestServerTypes(unittest.TestCase):

    # ------------------------------------------------------------------------------------------------------------------
    @classmethod
    def create_worker(cls, port, parent_proxy):
        # -- creates a worker and a proxy for it.
        handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())
        worker = DispatchWorkerTest(str(uuid.uuid4()), handler, ('localhost', port), parent_proxy)
        worker.register_interface_by_key('standard')
        worker.register_interface_by_key('cmd_utils')
        worker.start(blocking=False)
        return worker

    # ------------------------------------------------------------------------------------------------------------------
    def test_dispatch(self):
        server_port = clacks.get_new_port('localhost')

        server_handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())
        worker_handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())
        proxy_handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())

        server = clacks_dispatch.DispatchServer('Dispatch Server Test', worker_handler)

        server.register_handler('localhost', server_port, server_handler)
        server.register_interface_by_key('standard')
        server.register_interface_by_key('cmd_utils')
        server.start(blocking=False)

        parent_proxy = clacks.ClientProxyBase(('localhost', server_port), proxy_handler)

        for i in range(5):
            self.create_worker(clacks.get_new_port('localhost'), parent_proxy)

        time.sleep(0.5)

        # -- each call will be run on a new worker, but we call it directly on the server.
        # -- this will automatically redirect the function call to an available worker.
        identifiers = list()
        for i in range(10):
            # -- note how we call "foo" with a "dispatch_" prefix.
            response = server.get_command('dispatch_foo')()
            if response.traceback:
                raise Exception(response.traceback)
            identifiers.append(response.response)

        assert len(identifiers) > 0, 'Did not call any workers!'

        # -- test data type transfers and marshalling stability.
        # -- this makes use of the echo function to test how reliable a server is in transferring different object types
        for data in [
            ['list', 'of', 'stuff'],
            dict(some='key'),
            1,
            1.0,
            'string',
        ]:
            response_data = server.get_command('dispatch_echo')(data).response
            assert response_data == data, 'Server did not respond with same data (%s -> %s)!' % (data, response_data)

        server.end()

    # ------------------------------------------------------------------------------------------------------------------
    def test_dispatch_locked_worker(self):
        server_handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())
        proxy_handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())

        server = clacks_dispatch.DispatchServer(
            'Dispatch Server Test',
            server_handler,
        )
        server.register_interface_by_key('standard')
        server.register_interface_by_key('cmd_utils')

        server_port = clacks.get_new_port('localhost')
        server.register_handler('localhost', server_port, server_handler)

        server.start(blocking=False)

        parent_proxy = clacks.ClientProxyBase(('localhost', server_port), proxy_handler)
        parent_proxy.register_interface_by_type('standard')

        for i in range(5):
            self.create_worker(clacks.get_new_port('localhost'), parent_proxy)

        time.sleep(0.5)

        # -- by locking the worker, we run each call on the same worker instance, ensuring that until we unlock it
        # -- again, all calls are happening through the same proxy.
        parent_proxy.lock_worker()

        # -- each call will be run on the same worker.
        # -- because the worker is locked, all calls will be forwarded to that worker while it is locked.
        # -- this will automatically redirect the function call to an available worker.
        identifiers = list()
        for i in range(10):
            # -- note how we call "foo" with a "dispatch_" prefix.
            response = server.get_command('foo')()
            if response.traceback:
                raise Exception(response.traceback)
            identifiers.append(response.response)

        assert len(identifiers) > 0, 'Did not call any workers!'

        # -- test data type transfers and marshalling stability.
        # -- this makes use of the echo function to test how reliable a server is in transferring different object types
        for data in [
            ['list', 'of', 'stuff'],
            dict(some='key'),
            1,
            1.0,
            'string',
        ]:
            response_data = server.get_command('echo')(data)
            assert response_data.response == data, 'worker did not respond with same data (%s -> %s)!' % (data, response_data.response)

        parent_proxy.unlock_worker()

        assert len(identifiers) == 10, 'called function more than once'
        assert len(sorted(list(set(identifiers)))) == 1, 'Worker was not successfully locked!'

        server.end()

    # ------------------------------------------------------------------------------------------------------------------
    def test_broadcast_command(self):
        server_handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())
        proxy_handler = clacks.SimpleRequestHandler(clacks.SimplePackageMarshaller())

        server = clacks_dispatch.DispatchServer(
            'Dispatch Server Test',
            server_handler,
        )

        server.register_interface_by_key('standard')
        server.register_interface_by_key('cmd_utils')

        server_port = clacks.get_new_port('localhost')
        server.register_handler('localhost', server_port, server_handler)

        server.start(blocking=False)

        parent_proxy = clacks.ClientProxyBase(('localhost', server_port), proxy_handler)
        parent_proxy.register_interface_by_type('standard')

        workers = list()

        for i in range(5):
            workers.append(self.create_worker(clacks.get_new_port('localhost'), parent_proxy))

        time.sleep(0.5)

        server.broadcast_command('message', 'foobar')

        for worker in workers:
            assert len(worker.messages) == 1
            assert worker.messages[0] == 'foobar'

        server.end()
