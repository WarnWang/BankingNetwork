#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step02_generate_county_level_event_list
# @Date: 20/11/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step02_generate_county_level_event_list
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    data_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_fips.pkl'))
    branch_bank_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_branch_location_total_deposit_info.pkl'))

    county_event_df = {}
    for i in data_df.index:
        acq_9001 = data_df.loc[i, const.ACQ_9001]
        tar_9001 = data_df.loc[i, const.TAR_9001]
        event_id = data_df.loc[i, 'Deal_Number']
        year = data_df.loc[i, const.YEAR_MERGE]

        current_branch_df = branch_bank_df[branch_bank_df['year'] == year]

        acq_branch = current_branch_df[current_branch_df[const.COMMERCIAL_RSSD9001]]
