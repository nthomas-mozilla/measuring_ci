import os

import taskcluster


def tc_options():
    return {
        'rootUrl': os.environ.get('TASKCLUSTER_ROOT_URL', 'https://taskcluster.net'),
    }


def get_async_index():
    return taskcluster.aio.Index(options=tc_options())


def get_sync_index():
    return taskcluster.Index(options=tc_options())


def get_async_queue():
    return taskcluster.aio.Queue(options=tc_options())


def get_sync_queue():
    return taskcluster.Queue(options=tc_options())
