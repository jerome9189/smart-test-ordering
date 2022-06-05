#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


FILENAME = 'thinkaurelius@titan'


# In[3]:


df = pd.read_csv(f'rtp/{FILENAME}/{FILENAME}.csv')


# In[4]:


df = df[df['skipped'] == 0]
df['passed'] = df['failures'] + df['errors'] == 0


# In[5]:


df = df.drop(columns=['index', 'count', 'failures', 'errors', 'skipped'])


# In[6]:


df = df.groupby(['travisJobId', 'testName'], as_index=False).agg({'duration': 'mean', 'passed': 'all'})


# In[7]:


assert df.groupby(['travisJobId', 'testName'], as_index=False).count()['passed'].max()==1


# In[8]:


# specify number of tests
df_tmp = df.groupby(['testName'], as_index=False).count().sort_values(by='travisJobId', ascending=False)
max_runs = df_tmp['passed'].max()
df_tmp = df_tmp[df_tmp['passed'] >= 0.70*max_runs]
selected_tests = sorted(df_tmp['testName'].tolist())
selected_tests_set = set(selected_tests)


# In[9]:


df = df.groupby(['travisJobId'], as_index=False)[['testName', 'passed', 'duration']].agg(list)
df = df[df.apply(lambda row: selected_tests_set.issubset(set(row['testName'])), axis=1)]


# In[10]:


def get_test_name_status_duration_for_selected(name_list, passed_list, duration_list):
    selected_name_list = []
    selected_passed_list = []
    selected_duration_list = []
    for name,passed,duration in sorted(zip(name_list, passed_list, duration_list)):
        if name in selected_tests_set:
            selected_name_list.append(name)
            selected_passed_list.append(passed)
            selected_duration_list.append(duration)
    return selected_name_list, selected_passed_list, selected_duration_list

 


# In[11]:


statuses = pd.DataFrame(columns=selected_tests)
durations = pd.DataFrame(columns=selected_tests)
for _, row in df.iterrows():
    n,s,d = get_test_name_status_duration_for_selected(row['testName'], row['passed'], row['duration'])
    if all(s):
        continue
    statuses.loc[len(statuses)] = s
    durations.loc[len(durations)] = d


# In[13]:


statuses.to_csv(f'{FILENAME}_statuses.csv')
durations.to_csv(f'{FILENAME}_durations.csv')


# In[ ]:




