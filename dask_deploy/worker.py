from __future__ import print_function, division, absolute_import

import logging
import subprocess

from tornado import gen
from tornado.process import Subprocess
from . import controller

logger = logging.getLogger(__name__)


class SimpleWorkerHandler(object):
    """An example of a worker handler suitable for use with ``SimpleController``

    This is the simplest handler and illustrates minimum required behavior.
    It creates workers via subprocesses.

    The controller creates the scheduler, and the scheduler must create a
    scheduler file such as by using the ``--scheduler-file=...`` argument.
    The worker should reference the same scheduler file when it begins.
    In this class, we use a fixed value for the scheduler file.

    Another requirement is the worker must be assigned the given name.

    Additional arguments may be passed to the worker handler constructor.
    See ...

    """
    def __init__(self, name):
        self.proc = subprocess.Popen(
            'dask-worker',
            '--scheduler-file', controller.SimpleController.scheduler_file,
            '--death-timeout', '60',
            '--name', name,
        )

    def stop(self):
        self.proc.kill()


class WorkerHandler(object):
    def __init__(self, name, worker_command):
        self.command = worker_command + ['--name', name]
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(self.command)

    def stop(self):
        if self.proc is not None:
            return_code = self.proc.poll()
            if return_code is None:
                self.proc.kill()
                # return_code = self.proc.wait()


class AsyncWorkerHandler(object):
    def __init__(self, worker_command, name, **kwargs):
        if kwargs:
            logger.warning('Unused keyword arguments: %s', kwargs)
        self.command = [str(x) for x in worker_command + ['--name', name]]
        self.proc = None

    # @gen.coroutine
    def start(self):
        self.proc = Subprocess(self.command)

    @gen.coroutine
    def stop(self):
        if self.proc is not None:
            return_code = self.proc.proc.poll()
            if return_code is None:
                self.proc.proc.kill()
                return_code = yield self.proc.wait_for_exit(raise_error=False)
