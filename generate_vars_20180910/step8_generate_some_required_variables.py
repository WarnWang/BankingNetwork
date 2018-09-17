#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step8_generate_some_required_variables
# @Date: 17/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step8_generate_some_required_variables
"""

import os
from functools import partial

import numpy as np
import pandas as pd

from constants import Constants as const


def add_delta_change_group_change(tmp_df, data_key):
    result_df = tmp_df.copy().sort_values(by=const.YEAR, ascending=True)
    result_df.loc[:, '{}_chg'] = result_df[data_key].diff(periods=1)
    result_df.loc[:, '{}_pctchg'] = result_df[data_key].pct_change(periods=1)
    return result_df


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180911_third_part_concise_3018.pkl'))

    branch_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180913_ross_martin_fdic_76_16.pkl'))
    branch_state_df = branch_df[[const.FIPS_STATE_CODE, const.YEAR, const.COMMERCIAL_RSSD9001]].drop_duplicates()
    branch_state_count = branch_state_df.groupby([const.YEAR, const.COMMERCIAL_RSSD9001]).count().reset_index(
        drop=False).rename(index=str, columns={const.FIPS_STATE_CODE: const.BRANCH_STATE_NUM})

    # prepare branch state number count dataframe
    for prefix in [const.TAR, const.ACQ]:
        match_id = '{}_{}'.format(prefix, const.LINK_TABLE_RSSD9001)

        data_df[match_id].replace({'': np.nan}).dropna().apply(lambda x: str(int(x)))

        rename_dict = {const.COMMERCIAL_RSSD9001: match_id,
                       const.BRANCH_STATE_NUM: '{}_{}'.format(prefix, const.BRANCH_STATE_NUM)}

        tmp_branch_data = branch_state_count.rename(index=str, columns=rename_dict)
        data_df = data_df.merge(tmp_branch_data, on=[match_id, const.YEAR], how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180917_third_part_concise_3018_add_brstatenum.pkl'))

    # construct annual RCON5571 data file
    call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')

    call_dfs = []
    for year in range(1993, 2015):
        call_df = pd.read_pickle(os.path.join(call_report_path, 'call{}.pkl'.format(year)))
        useful_call_df = call_df[[const.COMMERCIAL_RSSD9001, const.SMALL_BUSINESS_LOAN]]
        useful_call_df.loc[:, const.YEAR] = year
        call_dfs.append(useful_call_df)

    sml_df = pd.concat(call_dfs, ignore_index=True, sort=False)
    sml_df.to_pickle(os.path.join(const.TEMP_PATH, '20180917_small_business_load_93_14.pkl'))

    # construct some loan related_variables
    add_change_to_sbl = partial(add_delta_change_group_change, data_key=const.SMALL_BUSINESS_LOAN)
