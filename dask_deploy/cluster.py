from __future__ import print_function, division, absolute_import

import itertools
import logging

from tornado import gen
from distributed.diagnostics.plugin import SchedulerPlugin
from .utils import resolve_qualname

logger = logging.getLogger(__name__)


class ClusterPlugin(SchedulerPlugin):
    def __init__(self, scheduler, cluster, strict=True, **kwargs):
        if kwargs:
            logger.warning('Unused keyword arguments: %s', kwargs)
        self.scheduler = scheduler
        self.cluster = cluster
        self.strict = strict

    @gen.coroutine
    def add_worker(self, scheduler, worker, **kwargs):
        name = self.scheduler.worker_info[worker].get('name', None)
        if name in self.cluster.worker_handlers:
            self.cluster.worker_addrs[worker] = name
        elif self.strict:
            logger.warning('Worker with unknown name: %r.  Closing.', name)
            yield self.scheduler.close_worker(worker=worker)

    @gen.coroutine
    def remove_worker(self, scheduler, worker, **kwargs):
        yield self.cluster.stop_worker(worker)


class Cluster(object):
    def __init__(self, scheduler, worker_handler_class, worker_handler_kwargs,
                 max_workers=None, identity='', **kwargs):
        if kwargs:
            logger.warning('Unused keyword arguments: %s', kwargs)
        if isinstance(worker_handler_class, str):
            worker_handler_class = resolve_qualname(worker_handler_class)
        self.worker_handler_class = worker_handler_class
        self.worker_handler_kwargs = worker_handler_kwargs
        self.is_closing = False
        self.scheduler = scheduler
        self.max_workers = max_workers
        self.identity = identity
        self._ids = itertools.count()
        self.worker_handlers = {}  # {name: WorkerHandler}
        self.worker_addrs = {}  # {address: name}

    @gen.coroutine
    def scale_up(self, n):
        """Bring the total count of workers up to ``n``"""
        if self.max_workers is not None:
            n = min(n, self.max_workers)
        yield [self.start_worker()
               for _ in range(n - len(self.worker_handlers))]

    @gen.coroutine
    def scale_down(self, workers):
        """Remove ``workers`` from the cluster"""
        yield [self.stop_worker(worker)
               for worker in set(workers) & set(self.worker_addrs)]

    @gen.coroutine
    def start_worker(self):
        if self.is_closing:
            return
        name = '%s-%s' % (self.identity, next(self._ids))
        handler = self.worker_handler_class(**dict(self.worker_handler_kwargs, name=name))
        self.worker_handlers[name] = handler
        if hasattr(handler, 'start'):
            yield handler.start()

    @gen.coroutine
    def stop_worker(self, worker):
        name = self.worker_addrs.pop(worker, None)
        handler = self.worker_handlers.pop(name, None)
        if handler is not None and hasattr(handler, 'stop'):
            yield handler.stop()

    @gen.coroutine
    def _stop_worker_gently(self, worker):
        yield self.scheduler.close_worker(worker=worker)
        yield gen.sleep(0.9)
        yield self.stop_worker(worker)

    def close(self):
        self.is_closing = True
        for worker in self.worker_addrs:
            self.scheduler.loop.add_callback(self._stop_worker_gently, worker)

    @property
    def scheduler_address(self):
        try:
            return self.scheduler.address
        except ValueError:
            return '<unstarted>'
