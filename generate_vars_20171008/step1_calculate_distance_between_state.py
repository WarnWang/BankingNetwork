#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step1_calculate_distance_between_state
# @Date: 8/10/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
from vincenty import vincenty

from constants import Constants as const

state_df = pd.read_csv(os.path.join(const.DATA_PATH, 'state_location_list.csv'))

state_number = state_df.shape[0]

new_df = pd.DataFrame(columns=['state1', 'state2', 'distance'])

for i in range(state_number):
    for j in range(i, state_number):
        state_combine = [state_df.iloc[i, 1], state_df.iloc[j, 1]]
        state_combine.sort()

        state1_geocode = (state_df.iloc[i, 2], state_df.iloc[i, 3])
        state2_geocode = (state_df.iloc[j, 2], state_df.iloc[j, 3])

        new_df.loc[new_df.shape[0]] = {'state1': state_combine[0],
                                       'state2': state_combine[1],
                                       'distance': vincenty(point1=state1_geocode, point2=state2_geocode, miles=False)}

new_df.to_pickle(os.path.join(const.TEMP_PATH, '20171008_distance_between_different_states.pkl'))
