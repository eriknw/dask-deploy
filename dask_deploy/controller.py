from __future__ import print_function, division, absolute_import

import atexit
import json
import logging
import os
import subprocess
import time
import uuid

from distributed.client import Client

logger = logging.getLogger(__name__)


class BaseController(object):
    def __init__(self):
        # Attributes you're likely to override
        self.scheduler_file = getattr(self, 'scheduler_file', None)
        self.worker_handler_class = getattr(self, 'worker_handler_class', None)
        self.worker_handler_kwargs = getattr(self, 'worker_handler_kwargs', {})
        self.adaptive_class = getattr(self, 'adaptive_class', 'distributed.deploy.Adaptive')
        self.adaptive_kwargs = getattr(self, 'adaptive_kwargs', {})
        self.setup_kwargs = getattr(self, 'setup_kwargs', {})
        # You probably don't need to override the default cluster and plugin classes
        self.cluster_class = getattr(self, 'cluster_class', 'dask_deploy.cluster.Cluster')
        self.cluster_kwargs = getattr(self, 'cluster_kwargs', {})
        self.plugin_class = getattr(self, 'plugin_class', 'dask_deploy.cluster.ClusterPlugin')
        self.plugin_kwargs = getattr(self, 'plugin_kwargs', {})
        self._client = getattr(self, '_client', None)
        self._is_running = False

    def launch_scheduler(self):
        raise NotImplementedError

    def close_scheduler(self):
        raise NotImplementedError

    @property
    def client(self):
        if self._client is None:
            if self.scheduler_file is None:
                error_message = '%s.scheduler_file is None.' % type(self).__name__
                if self._is_running is None:
                    error_message += '  Maybe call %s.start() first' % type(self).__name__
                raise ValueError(error_message)
            self._client = Client(scheduler_file=self.scheduler_file)
        return self._client

    @property
    def identity(self):
        return self.scheduler_file.rsplit('-')[-1]

    @property
    def worker_file(self):
        return os.path.join(
            os.path.dirname(self.scheduler_file),
            'dask_worker-%s' % self.identity
        )

    def start(self):
        self.launch_scheduler()
        atexit.register(self.close)
        self._is_running = True

        def ensure_string(attr):
            val = getattr(self, attr)
            if val is None and attr in ('adaptive_class',):
                return
            if not isinstance(val, str):
                raise ValueError('%s.%s should be a string; got type %s instead'
                                 % (type(self).__name__, attr, type(val)))

        try:
            for attr in ('scheduler_file', 'adaptive_class', 'cluster_class',
                         'plugin_class', 'worker_handler_class'):
                ensure_string(attr)

            with open(self.worker_file, 'w') as fp:
                json.dump(
                    {
                        'adaptive_class': self.adaptive_class,
                        'adaptive_kwargs': self.adaptive_kwargs,
                        'cluster_class': self.cluster_class,
                        'cluster_kwargs': self.cluster_kwargs,
                        'plugin_class': self.plugin_class,
                        'plugin_kwargs': self.plugin_kwargs,
                        'worker_handler_class': self.worker_handler_class,
                        'worker_handler_kwargs': self.worker_handler_kwargs,
                        'setup_kwargs': self.setup_kwargs,
                    },
                    fp,
                )
        except Exception:
            self.close()
            self._is_running = False
            raise

    def add_workers(self, n):
        """Add ``n`` workers``"""
        if not self._is_running:
            self.start()

        def _add_workers(n, dask_scheduler):
            cluster = dask_scheduler._cluster_
            cluster.scale_up(n + len(cluster.worker_handlers))

        self.client.run_on_scheduler(_add_workers, n)

    def scale_up(self, n):
        """Bring the total count of workers up to ``n``"""
        if not self._is_running:
            self.start()

        def _scale_up(n, dask_scheduler):
            dask_scheduler._cluster_.scale_up(n)

        self.client.run_on_scheduler(_scale_up, n)

    def close(self):
        if not self._is_running:
            return
        try:
            self.client.run_on_scheduler(lambda dask_scheduler: dask_scheduler._cluster_.close())
            time.sleep(0.5)
            self.client.loop.add_callback(self.client.scheduler.retire_workers, close_workers=True)
            time.sleep(0.5)
            self.client.loop.add_callback(self.client.scheduler.terminate)
            time.sleep(0.5)
            self.client.run_on_scheduler(lambda dask_scheduler: dask_scheduler.loop.stop())
            time.sleep(0.5)
        except Exception:
            pass
        try:
            self.close_scheduler()
        except NotImplementedError:
            pass
        self._is_running = False
        atexit.unregister(self.close)

    def __enter__(self):
        if not self._is_running:
            self.start()
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class SimpleController(BaseController):
    scheduler_file = './dask_scheduler-static'
    worker_handler_class = 'dask_deploy.worker.SimpleWorkerHandler'

    def __init__(self):
        super(SimpleController, self).__init__()
        self.scheduler_proc = None

    def launch_scheduler(self):
        scheduler_command = [
            'dask-scheduler',
            '--port', '0',
            '--scheduler-file', self.scheduler_file,
            '--preload', 'dask_deploy.run_on_scheduler',
        ]
        self.scheduler_proc = subprocess.Popen(scheduler_command)

    def close_scheduler(self):
        if self.scheduler_proc is not None:
            self.scheduler_proc.kill()


class SubprocessController(BaseController):
    worker_handler_class = 'dask_deploy.worker.AsyncWorkerHandler'

    def __init__(self, shared_directory='./cache', cache_directory='./cache', num_workers=1,
                 max_workers=None, threads_per_worker=1, add_to_worker_command=()):
        super(SubprocessController, self).__init__()
        identity = uuid.uuid1().hex[:10]
        self.scheduler_file = os.path.join(shared_directory, 'dask_scheduler-%s' % identity)
        self.scheduler_proc = None
        if not os.path.exists(shared_directory):
            os.makedirs(shared_directory)
        self.cache_directory = cache_directory
        self.worker_handler_kwargs['worker_command'] = [
            'dask-worker',
            '--scheduler-file', self.scheduler_file,
            '--death-timeout', '60',
            '--local-directory', self.cache_directory,
            '--nthreads', str(threads_per_worker),
        ] + list(add_to_worker_command)
        self.cluster_kwargs['max_workers'] = max_workers
        self.setup_kwargs['num_workers'] = num_workers

    def launch_scheduler(self):
        scheduler_command = [
            'dask-scheduler',
            '--port', '0',
            '--scheduler-file', self.scheduler_file,
            '--local-directory', self.cache_directory,
            '--preload', 'dask_deploy.run_on_scheduler',
        ]
        self.scheduler_proc = subprocess.Popen(scheduler_command)

    def close_scheduler(self):
        if self.scheduler_proc is not None:
            self.scheduler_proc.kill()
