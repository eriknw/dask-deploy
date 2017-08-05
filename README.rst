Dask-Deploy
===========

Construction in progress...

Getting Started
---------------

.. code-block:: Python

    >>> from dask_deploy.controller import SubprocessController
    >>> controller = SubprocessController()
    >>> controller.start()
    >>> controller.scale_up(4)
    >>> controller.client
    <Client: scheduler='tcp://192.168.0.103:50481' processes=4 cores=4>

