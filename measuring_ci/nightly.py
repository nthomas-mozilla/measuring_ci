import asyncio
import logging
from datetime import datetime

import taskcluster
import yaml

from measuring_ci.taskcluster_utils import (
    get_async_queue, get_sync_queue, get_async_index
)

log = logging.getLogger()


def sanitize_date(date):
    """Attempt to sanitize a date format.

    Should ideally cope with epoch and also validate strings.
    """
    if isinstance(date, datetime):
        return date.strftime("%Y.%m.%d")
    return date


async def fetch_nightlies(date, project='mozilla-central'):
    """Fetch nightly task group IDs by date."""
    index = "gecko.v2.{project}.nightly.{date}.revision"

    index = index.format(
        project=project.split('/')[-1],  # remove paths like release/ integration/
        date=sanitize_date(date),
    )

    idx = get_async_index()
    queue = get_async_queue()

    ret = await idx.listNamespaces(index)

    revision_namespaces = [n['namespace'] for n in ret['namespaces']]

    tasks = [asyncio.ensure_future(idx.listNamespaces(n)) for n in revision_namespaces]
    ret = await asyncio.gather(*tasks)
    product_namespaces = [n['namespace'] for m in ret for n in m.get('namespaces', [])]

    # -l10n namespaces are filtered out here as they have another namespace layer
    # underneath, not tasks.
    tasks = [asyncio.ensure_future(idx.listTasks(n)) for n in product_namespaces]
    ret = await asyncio.gather(*tasks)
    build_tasks = list()
    for platform in ret:
        if not platform.get('tasks'):
            continue
        # All of the platforms within a product should have tasks
        # that fall under the same task group ID, so we can just
        # get the first one and use that.
        build_tasks.append(platform['tasks'][0]['namespace'])

    results = dict()
    for task in build_tasks:
        log.debug('Looking for taskId via task %s', task)
        try:
            build_task = await idx.findTask(task)
            task_def = await queue.task(build_task['taskId'])
            revision, product = task.split('.')[-3:-1]
            results[task_def['taskGroupId']] = {
                'product': product,
                'revision': revision,
            }
            # Find the version, hidden in the parameters of the
            # decision task's artifacts
            sync_queue = get_sync_queue()
            parameters_response = sync_queue.getLatestArtifact(taskId=task_def['taskGroupId'], name='public/parameters.yml')
            # print(parameters_response)

            parameters = yaml.load(parameters_response['response'].text)
            # async variant
            # parameters = yaml.loads(await parameters_response['response'].text())
            results[task_def['taskGroupId']]['version'] = parameters.get('app_version', '')

        except taskcluster.exceptions.TaskclusterRestFailure as e:
            log.warning(e)

    return results
