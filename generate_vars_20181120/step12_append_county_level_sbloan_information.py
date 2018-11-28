#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step12_append_county_level_sbloan_information
# @Date: 2018-11-28
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step12_append_county_level_sbloan_information
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    cra_df: DataFrame = pd.read_stata(os.path.join(const.DATA_PATH, 'CRA_Data', '9616exp_discl_table_d11_bankcnty.dta'))
    cra_df.loc[:, const.FIPS] = cra_df.apply(lambda x: int(x['state']) * 1000 + int(x['county']), axis=1)
    cra_df.loc[:, const.SB_LOAN] = cra_df.apply(
        lambda x: x['amt_orig_lt100k'] + x['amt_orig_100_250k'] + x['amt_orig_gt250k'], axis=1)
    cra_df.loc[:, '{}Num'.format(const.SB_LOAN)] = cra_df.apply(
        lambda x: x['num_orig_lt100k'] + x['num_orig_100_250k'] + x['num_orig_gt250k'], axis=1)
    cra_df.loc[:, const.RSSD9001] = cra_df['rssdid'].apply(lambda x: str(int(x.strip())) if x.isdigit() else np.nan)
    cra_valid_df: DataFrame = cra_df[cra_df[const.RSSD9001].notnull()].copy()
    cra_valid_df = cra_valid_df[cra_valid_df[const.RSSD9001] != '0'].copy()
    useful_cols = ['activity_year', 'num_orig_lt100k', 'amt_orig_lt100k', 'num_orig_100_250k', 'amt_orig_100_250k',
                   'num_orig_gt250k', 'amt_orig_gt250k', 'num_orig_rev1m', 'amt_orig_rev1m', 'num_orig_aff',
                   'amt_orig_aff', const.RSSD9001, const.FIPS, const.SB_LOAN, '{}Num'.format(const.SB_LOAN)]

    event_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181128_event_count_list_county_info.pkl'))
    for prefix in [const.TAR, const.ACQ]:
        merge_key = const.TAR_9001 if prefix == const.TAR else const.ACQ_9001

        useful_cra_df = cra_valid_df[useful_cols].copy()
        rename_dict = {const.RSSD9001: merge_key, 'activity_year': const.YEAR_MERGE, const.FIPS: const.FIPS}

        for key in useful_cols:
            if key in rename_dict:
                continue
            if key == const.SB_LOAN:
                rename_dict[key] = '{}_{}_ct'.format(prefix, key)
            else:
                rename_dict[key] = '{}_{}'.format(prefix, key)

        county_branch_renamed = useful_cra_df.rename(index=str, columns=rename_dict)

        event_df = event_df.merge(county_branch_renamed, on=[const.YEAR_MERGE, const.FIPS, merge_key], how='left')

    drop_keys = ['Tar_num_orig_rev1m', 'Tar_amt_orig_rev1m', 'Tar_num_orig_aff', 'Tar_amt_orig_aff',
                 'Acq_num_orig_rev1m', 'Acq_amt_orig_rev1m', 'Acq_num_orig_aff', 'Acq_amt_orig_aff']

    event_df = event_df.drop(drop_keys, axis=1)

    event_df.to_pickle(os.path.join(const.TEMP_PATH, '20181128_third_part_branch_county_data.pkl'))
    event_df.to_stata(os.path.join(const.RESULT_PATH, '20181128_third_part_branch_county_data.dta'),
                      write_index=False)
