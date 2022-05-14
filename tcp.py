import pandas as pd
import itertools
from functools import cache
from random import sample


# data loading, initialization
test_cases = ['translations', 'test:macos', 'test:linux',
              'inkscape:windows:x64', 'inkscape:macos', 'appimage:linux', 'inkscape:linux']

statuses_df = pd.read_csv('statuses_big.csv', index_col=[0])
durations_df = pd.read_csv('durations_big.csv', index_col=[0])
failure_times = {}
success_times = {}
for case in test_cases:
    failed_time_col = durations_df[statuses_df[case] == False][case]
    success_time_col = durations_df[statuses_df[case] == True][case]
    if len(failed_time_col) > 0:
        failure_times[case] = failed_time_col.iloc[0]
    else:
        failure_times[case] = success_time_col.iloc[0]
    if len(success_time_col) > 0:
        success_times[case] = success_time_col.iloc[0]
    else:
        success_times[case] = failed_time_col.iloc[0]


@cache
def get_scenario_probability(sequence_until_failure):
    """Compute probability of all test cases but the last one passing for a given
    sequence of test cases

    Args:
        sequence_until_failure (list): order of test case execution until first failure
    """
    query = f'`{sequence_until_failure[-1]}`==False'
    if len(sequence_until_failure) > 1:
        query += ' & '.join([' '] + [f'`{case}`==True' for case in sequence_until_failure[:-1]])
    statuses_df_filtered = statuses_df.query(query)
    probability = len(statuses_df_filtered) / len(statuses_df)
    return probability


def get_expected_ttff(order, best_time=float('inf')):
    """Returns expected value of the time till first failure (ttff) for a given
    test suite ordering

    Args:
        order (list): a permutation of the test suite
        best_time (float): optional, the value of the best expected ttff found so far
    """
    cur_time = 0
    for i, case in enumerate(order):
        scenario_probability = get_scenario_probability(order[:i+1])
        if scenario_probability != 0:
            cur_time += (failure_times[case] + sum([success_times[x]
                     for x in order[:i]])) * scenario_probability
        else:
            continue
        if cur_time >= best_time:
            return float('inf')
    return cur_time


def get_best_order():
    best_order = test_cases
    best_time = float('inf')
    for order in itertools.permutations(best_order):
        cur_time = get_expected_ttff(order, best_time)
        if cur_time < best_time:
            best_order = order
            best_time = cur_time
    return best_order


def get_true_mean_ttff(order):
    """Computes and returns the average ttff over the dataset, given a particular
    test suite execution order

    Args:
        order (list): a permutation of the test suite
    """
    mean_ttff = 0
    for status_row, duration_row in zip(statuses_df.iterrows(), durations_df.iterrows()):
        for case in order:
            mean_ttff += duration_row[1][case]
            # if this test case failed
            if not status_row[1][case]:
                break
    return mean_ttff / len(statuses_df)

if __name__ == '__main__':
    best_order = get_best_order()
    print(f'best ordering = {best_order}')
    print(f'mean ttff for best ordering = {get_true_mean_ttff(best_order)}')
    print(f'mean ttff for default ordering = {get_true_mean_ttff(test_cases)}')
    avg_mean_ttf_random = 0
    for i in range(50):
        avg_mean_ttf_random += get_true_mean_ttff(sample(test_cases, len(test_cases)))
    avg_mean_ttf_random /= 50
    print(f'(averaged) mean ttff for random ordering = {avg_mean_ttf_random}')