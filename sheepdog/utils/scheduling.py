from Queue import Queue, Full
from threading import Thread

import cdispyutils

from sheepdog.errors import (
    InternalError,
)
from sheepdog.globals import (
    ASYNC_MAX_Q_LEN,
    ERR_ASYNC_SCHEDULING,
)


logger = cdispyutils.log.get_logger("submission.scheduling")


def async_pool_consumer(task_queue):
    task = task_queue.get()
    while task:
        try:
            task.target(*task.args, **task.kwargs)
        except Exception as e:  # pylint: disable=broad-except
            logger.exception(e)
        finally:
            task = task_queue.get()


class AsyncPoolTask(object):
    """Represents an async task."""

    def __init__(self, target, *args, **kwargs):
        """
        Args:
            target: Target function
            *args: Target function positional args
            **kwargs: Target function key word args
        """
        self.target = target
        self.args = args
        self.kwargs = kwargs


class AsyncPool(object):
    """Creates a pool of workers that allows async scheduling."""

    def __init__(self, worker_class=Thread, max_queue_len=ASYNC_MAX_Q_LEN):
        self.worker_class = worker_class
        self.task_queue = Queue(max_queue_len)
        self.workers = []

    def start(self, n_workers):
        """Send a NoneType to all workers requesting exit."""
        self.grow(n_workers)

    def schedule(self, function, *args, **kwargs):
        """Add a task to the queue"""
        try:
            self.task_queue.put_nowait(AsyncPoolTask(
                function,
                *args,
                **kwargs
            ))
        except Full:
            raise InternalError(ERR_ASYNC_SCHEDULING)

    def close(self):
        """Send a NoneType to all workers requesting exit"""
        self.shrink(len(self.workers))

    def grow(self, n_workers):
        """
        Add and start workers to the scheduling pool. Note: workers are
        started immediately.
        """
        workers = [
            self.worker_class(
                target=async_pool_consumer,
                args=(self.task_queue,),
            )
            for _ in range(n_workers)
        ]

        for worker in workers:
            worker.daemon = True
            worker.start()

        self.workers.extend(workers)

    def shrink(self, n_workers):
        """Send a NoneType to `n_workers` workers requesting exit."""
        for worker in range(n_workers):
            self.task_queue.put(None)

    def join(self):
        """Wait for all workers to finish"""
        for worker in self.workers:
            worker.join()
