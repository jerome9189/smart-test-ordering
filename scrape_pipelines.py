import requests
from os.path import exists
import json
import pandas as pd
from tqdm import tqdm


def get_pipelines():
    # check if pipeline data is available locally
    if exists('pipelines.json'):
        with open('pipelines.json') as f:
            return json.load(f)
    i = 1
    pipelines = list()
    while True:
        resp = requests.get(
            f'https://gitlab.com/api/v4/projects/3472737/pipelines?page={i}&per_page=100')
        resp_pipelines = [[x['id'], x['status']]
                          for x in resp.json() if x['status'] in ['success', 'failed']]
        if not resp_pipelines:
            break
        else:
            pipelines.extend(resp_pipelines)
        if i == 50:
            break
        i += 1

    # save pipelines to a file
    with open('pipelines.json', 'w') as f:
        json.dump(pipelines, f)
    return pipelines


def get_job_data(pipeline_id, required_jobs):
    resp = requests.get(f'https://gitlab.com/api/v4/projects/3472737/pipelines/{pipeline_id}/jobs')
    resp_json = resp.json()
    resp_dict = {x['name']: x for x in resp_json}
    # check if the pipeline ran all required jobs
    for req_job in required_jobs:
        if req_job not in resp_dict or resp_dict[req_job]['status'] not in ['success', 'failed']:
            return None, None
    return [resp_dict[x]['status'] == 'success' for x in required_jobs], [resp_dict[x]['duration'] for x in required_jobs]


if __name__ == '__main__':
    pipelines = get_pipelines()
    jobs_dict = dict()
    required_jobs = ['translations', 'test:macos', 'test:linux',
                     'inkscape:windows:x64', 'inkscape:macos', 'appimage:linux', 'inkscape:linux']
    statuses_list = list()
    durations_list = list()
    for p in tqdm(pipelines[:10000]):
        statuses, durations = get_job_data(p[0], required_jobs)
        if not statuses:
            continue
        statuses_list.append(statuses)
        durations_list.append(durations)
    statuses_df = pd.DataFrame(statuses_list, columns=required_jobs)
    durations_df = pd.DataFrame(durations_list, columns=required_jobs)
    indexer = statuses_df.apply(lambda row: not all([x for x in row]), axis=1)
    statuses_df = statuses_df.loc[indexer]
    durations_df = durations_df.loc[indexer]
    statuses_df.to_csv('statuses_big_with_failures.csv')
    durations_df.to_csv('durations_big_with_failures.csv')
