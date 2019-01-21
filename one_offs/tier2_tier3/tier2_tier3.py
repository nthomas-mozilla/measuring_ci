import argparse
import asyncio
import copy
import logging
import os
import json
from datetime import datetime

import pandas as pd
import yaml

from measuring_ci.costs import fetch_all_worker_costs, taskgraph_cost
from measuring_ci.files import open_wrapper
from taskhuddler.graph import TaskGraph

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("s3fs").setLevel(logging.INFO)


def parse_args():
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--project', type=str, default='mozilla-central')
    parser.add_argument('--product', type=str, default='firefox')
    parser.add_argument('--config', type=str, default='scanner.yml')
    return parser.parse_args()


def consider_push(timestamp):
    """Guess at whether a revision's CI tasks have finished by now."""
    timestamp = datetime.fromtimestamp(timestamp)
    # whoop whoop, hardcoded dates
    if timestamp >= datetime(2019, 1, 1, 0, 0) and timestamp < datetime(2019, 1, 20, 0, 0):
        return True
    return False


def find_push_by_group(group_id, project, pushes):
    return next(push for push in pushes[project] if pushes[project][push]['taskgraph'] == group_id)


async def _semaphore_wrapper(action, args, semaphore):
    async with semaphore:
        return await action(*args)


async def scan_project(project, product, config):
    """Scan a project's recent history for complete task graphs."""
    config = copy.deepcopy(config)
    cost_dataframe_columns = [
        'project', 'product', 'groupid',
        'pushid', 'graph_date', 'origin',
        'totalcost', 'tier23_cost', 'taskcount', 'tier23_taskcount',
    ]

    short_project = project.split('/')[-1]
    config['cost_output'] = config['cost_output'].format(project=short_project)
    config['pushlog_cache_file'] = config['pushlog_cache_file'].format(
        project=project.replace('/', '_'))

    log.info("Looking up pushlog for {}".format(project))
    log.debug('Loading pushlog cache')
    with open_wrapper(config['pushlog_cache_file'], 'r') as f:
        pushes = json.load(f)

    try:
        existing_costs = pd.read_parquet(config['cost_output'] + '.parquet')
        log.info("Loaded existing per-push costs")
    except Exception:
        log.info("Couldn't load existing per-push costs, using empty data set")
        existing_costs = pd.DataFrame(columns=cost_dataframe_columns)

    graphs = list()

    count_no_graph_id = 0
    count_ignored = 0
    for push in pushes[project]:
        log.debug("Examining push %s", push)
        if str(push) in existing_costs['pushid'].values:
            log.info("Already costed push %s, skipping.", push)
            continue
        if consider_push(pushes[project][push]['date']):
            graph_id = pushes[project][push]['taskgraph']
            if not graph_id or graph_id == '':
                log.debug("Couldn't find graph id for {} push {}".format(project, push))
                count_no_graph_id += 1
                continue
            log.debug("Push %s, Graph ID: %s", push, graph_id)
            graphs.append(graph_id)
        else:
            # log.debug("Push %s probably not of interest, skipping", push)
            count_ignored += 1
    log.info('{} pushes without a graph_id; ignoring {} '.format(
        count_no_graph_id, count_ignored))

    log.info('{} task graphs to consider'.format(len(graphs)))

    costs = list()

    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )
    log.info('Calculating costs')
    write_counter = 0
    def write_output(costs, existing_costs):
        costs_df = pd.DataFrame(costs, columns=cost_dataframe_columns)

        new_costs = existing_costs.merge(costs_df, how='outer')
        new_costs.to_parquet(config['cost_output'] + '.parquet', compression='gzip')
        new_costs.to_csv(config['cost_output'] + '.csv')
        log.info('Wrote cost data')
        return new_costs

    # multiprocessing would be a big speed up here
    for graph_id in graphs:
        log.info('Starting graph %s', graph_id)
        graph = TaskGraph(graph_id)
        push = find_push_by_group(graph.groupid, project, pushes)

        full_cost, final_runs_cost = taskgraph_cost(graph, worker_costs)
        task_count = len([t for t in graph.tasks()])

        graph.tasklist = [t for t in graph.tasks() if t.tier in (2, 3)]
        tier23_cost, _ = taskgraph_cost(graph, worker_costs)
        tier23_task_count = len(graph.tasklist)

        costs.append(
            [
                short_project,
                product,
                graph.groupid,
                push,
                pushes[project][push]['date'],
                'push',
                round(full_cost, 2),
                round(tier23_cost, 2),
                task_count,
                tier23_task_count,
            ]
        )
        write_counter += 1

        if (write_counter % 50 == 0):
            existing_costs = write_output(costs, existing_costs)
            costs = list()
            log.info('%s done, %s to go', write_counter, len(graphs) - write_counter)

    write_output(costs, existing_costs)



async def main(args):
    with open(args['config'], 'r') as y:
        config = yaml.load(y)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    log.info('TC_CACHE_DIR is %s' % config['TC_CACHE_DIR'])

    # cope with original style, listing one project, or listing multiple
    for project in args.get('projects', [args.get('project')]):
        await scan_project(project, args['product'], config)


def lambda_handler(args, context):
    assert context  # not current used
    if 'config' not in args:
        args['config'] = 'scanner.yml'
    if 'product' not in args:
        args['product'] = 'firefox'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {'dummy': 1})
