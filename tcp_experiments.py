import pandas as pd
import itertools
from functools import cache
from random import sample
from tqdm import tqdm

alpha = 1

# data loading, initialization
test_cases = ['com.thinkaurelius.titan.blueprints.BerkeleyJEBlueprintsTest',
 'com.thinkaurelius.titan.blueprints.InternalCassandraBlueprintsTest',
 'com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyDBjeKeyColumnValueTest',
 'com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyDBjeKeyColumnValueVariableTest',
 'com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEKeyValueTest',
 'com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJeHashKeyColumnValueTest',
 'com.thinkaurelius.titan.diskstorage.cassandra.UUIDTest',
 'com.thinkaurelius.titan.diskstorage.locking.LocalLockMediatorTest',
 'com.thinkaurelius.titan.diskstorage.util.StorageFeaturesTest',
 'com.thinkaurelius.titan.graphdb.astyanax.InternalAstyanaxGraphTest',
 'com.thinkaurelius.titan.graphdb.berkeleyje.BerkeleyJEGraphTest',
 'com.thinkaurelius.titan.graphdb.idmanagement.IDManagementTest',
 'com.thinkaurelius.titan.graphdb.idmanagement.IDPoolTest',
 'com.thinkaurelius.titan.graphdb.idmanagement.VariableLongTest',
 'com.thinkaurelius.titan.graphdb.idmanagement.VertexIDAssignerTest',
 'com.thinkaurelius.titan.graphdb.serializer.ByteBufferTest',
 'com.thinkaurelius.titan.graphdb.serializer.KryoTest',
 'com.thinkaurelius.titan.graphdb.serializer.SerializerTest',
 'com.thinkaurelius.titan.util.datastructures.BitMapTest',
 'com.thinkaurelius.titan.util.datastructures.RandomRemovalListTest']

FILENAME = 'thinkaurelius@titan'
statuses_df = pd.read_csv(f'{FILENAME}_statuses.csv', index_col=[0])
durations_df = pd.read_csv(f'{FILENAME}_durations.csv', index_col=[0])
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
    d = 2 ** len(sequence_until_failure)
    probability = (len(statuses_df_filtered) + alpha) / (len(statuses_df) + (alpha * d))
    return probability

@cache
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
        if scenario_probability == 0:
            continue
        cur_time += (failure_times[case] + sum([success_times[x]
                                                for x in order[:i]])) * scenario_probability
        if cur_time >= best_time:
            return float('inf')
    return cur_time


def get_em_order():
    best_order = test_cases
    best_time = float('inf')
    # for order in tqdm(itertools.permutations(best_order), total=math.factorial(len(test_cases))):
    for order in itertools.permutations(best_order):
        cur_time = get_expected_ttff(order, best_time)
        if cur_time < best_time:
            best_order = order
            best_time = cur_time
    return best_order


def get_greedy_em_order():
    all_cases = set(test_cases)
    result = list()
    while all_cases:
        min_term = float('inf')
        min_case = list(all_cases)[0]
        for case in all_cases:
            scenario_probability = get_scenario_probability(tuple(result + [case]))
            term = (failure_times[case] +
                         sum([success_times[x] for x in result])) * scenario_probability
            if term < min_term:
                min_term = term
                min_case = case
        all_cases.remove(min_case)
        result.append(min_case)
    return result


def get_greedy_probabilistic_order():
    all_cases = set(test_cases)
    result = list()
    while all_cases:
        max_prob = -1
        case_most_likely_to_fail = list(all_cases)[0]
        for case in all_cases:
            scenario_probability = get_scenario_probability(tuple(result + [case]))
            if scenario_probability > max_prob:
                max_prob = scenario_probability
                case_most_likely_to_fail = case
        all_cases.remove(case_most_likely_to_fail)
        result.append(case_most_likely_to_fail)
    return tuple(result)


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

def get_best_mean_ttff():
    mean_ttff = 0
    for status_row, duration_row in zip(statuses_df.iterrows(), durations_df.iterrows()):
        min_time = float('inf')
        for case in test_cases:
            if not status_row[1][case]:
                min_time = min(min_time, duration_row[1][case])
        mean_ttff += min_time
        # for case in order:
        #     mean_ttff += duration_row[1][case]
        #     # if this test case failed
        #     if not status_row[1][case]:
        #         break
    return mean_ttff / len(statuses_df)


if __name__ == '__main__':
    print('-'*10)
    greedy_em_order = get_greedy_em_order()
    # # uncomment this to see order
    # # print(f'greedy em ordering = {greedy_em_order}')
    print(f'mean ttff for greedy em ordering = {get_true_mean_ttff(greedy_em_order)}')
    print('-'*10)
    greedy_probabilistic_order = get_greedy_probabilistic_order()
    # print(f'greedy probabilistic ordering = {greedy_probabilistic_order}')
    print(f'mean ttff for greedy probabilistic ordering = {get_true_mean_ttff(greedy_probabilistic_order)}')
    print('-'*10)
    avg_mean_ttf_random = 0
    for i in range(50):
        avg_mean_ttf_random += get_true_mean_ttff(sample(test_cases, len(test_cases)))
    avg_mean_ttf_random /= 50
    print(f'(averaged) mean ttff for random ordering = {avg_mean_ttf_random}')
    print('-'*10)
    print(f'(averaged) best possible ttff for each test suite execution = {get_best_mean_ttff()}')
    print('-'*10)
    em_order = get_em_order()
    # print(f'em ordering = {em_order}')
    print(f'mean ttff for em ordering = {get_true_mean_ttff(em_order)}')
    print('-'*10)
