import traceback


# ----------------------------------------------------------------------------------------------------------------------
def lock_worker(func):
    """
    Decorator that may be used to lock the current acquired worker such that a given proxy command can ensure that all
    its dispatched commands are run on the same worker.

    This is super useful when running a series of proxy commands that are strung together based on server I/O, such as
    sending a file to the server, telling the server to do something with it, and then returning the result, which
    might be a download of another file.

    The "lock worker" mechanism ensures that while the function decorated with it is being run, even during nested
    calls, the same worker proxy will be engaged.

    This allows a developer to simply lock the worker without needing to worry about which one the server will engage
    for the task in question.
    """
    def wrapper(*args, **kwargs):
        cls = args[0]
        locked = cls.lock_worker()

        # -- raise an exception if a worker could not be acquired.
        if locked is not None and locked.traceback:
            raise Exception(locked.traceback)

        try:
            result = func(*args, **kwargs)
        except Exception:
            cls.unlock_worker()
            raise Exception(traceback.format_exc())

        cls.unlock_worker()
        return result

    return wrapper
