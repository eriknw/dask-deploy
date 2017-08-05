from __future__ import print_function, division, absolute_import

import json
import logging
import os
import time

from dask_deploy.utils import resolve_qualname

logger = logging.getLogger(__name__)


def dask_setup(scheduler):
    identity = scheduler.scheduler_file.rsplit('-', 1)[-1]
    prefix = os.path.dirname(scheduler.scheduler_file)
    worker_file = os.path.join(prefix, 'dask_worker-%s' % identity)
    give_up_time = time.time() + 10.0
    while not os.path.exists(worker_file) and time.time() < give_up_time:
        time.sleep(0.1)
    if not os.path.exists(worker_file):
        # XXX: should we close the scheduler?
        raise ValueError('Worker file not found: %s' % worker_file)
    with open(worker_file) as fp:
        worker_info = json.load(fp)

    cluster_class = resolve_qualname(worker_info['cluster_class'])
    cluster_kwargs = dict(
        worker_info['cluster_kwargs'],
        scheduler=scheduler,
        identity=identity,
        worker_handler_class=resolve_qualname(worker_info['worker_handler_class']),
        worker_handler_kwargs=worker_info['worker_handler_kwargs'],
    )
    cluster = cluster_class(**cluster_kwargs)
    scheduler._cluster_ = cluster

    plugin_class = resolve_qualname(worker_info['plugin_class'])
    plugin_kwargs = dict(
        worker_info['plugin_kwargs'],
        scheduler=scheduler,
        cluster=cluster,
    )
    plugin = plugin_class(**plugin_kwargs)
    scheduler.add_plugin(plugin)
    scheduler._plugin_ = plugin

    adaptive_class = resolve_qualname(worker_info['adaptive_class'])
    if adaptive_class is not None:
        adaptive_kwargs = dict(
            worker_info['adaptive_kwargs'],
            cluster=cluster,
            scheduler=scheduler,
        )
        adaptive_cluster = adaptive_class(**adaptive_kwargs)
        scheduler._adaptive_cluster_ = adaptive_cluster

    num_workers = worker_info.get('setup_kwargs').get('num_workers')
    if num_workers is not None:
        cluster.scale_up(num_workers)
