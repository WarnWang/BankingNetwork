#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step8_count_branch_state_number
# @Date: 17/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step8_count_branch_state_number
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const


def add_delta_change_group_change(tmp_df, data_key):
    result_df = tmp_df.copy().sort_values(by=const.YEAR, ascending=True)
    result_df.loc[:, '{}_chg'.format(data_key)] = result_df[data_key].diff(periods=1)
    result_df.loc[:, '{}_pctchg'.format(data_key)] = result_df[data_key].pct_change(periods=1)
    return result_df


ACQ_9001 = '{}_{}'.format(const.ACQ, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TAR, const.LINK_TABLE_RSSD9001)

if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180911_third_part_concise_3018.pkl'))

    branch_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180913_ross_martin_fdic_76_16.pkl'))
    branch_state_df = branch_df[[const.FIPS_STATE_CODE, const.YEAR, const.COMMERCIAL_RSSD9001]].drop_duplicates()
    branch_state_count = branch_state_df.groupby([const.YEAR, const.COMMERCIAL_RSSD9001]).count().reset_index(
        drop=False).rename(index=str, columns={const.FIPS_STATE_CODE: const.BRANCH_STATE_NUM})

    rssd_link_file = branch_df[[const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364, const.YEAR]].copy().dropna(
        how='any').drop_duplicates()
    branch_state_count2 = branch_state_count.merge(rssd_link_file, on=[const.COMMERCIAL_RSSD9001, const.YEAR],
                                                   how='left')

    # prepare branch state number count dataframe
    for prefix in [const.TAR, const.ACQ]:
        match_id = '{}_{}'.format(prefix, const.LINK_TABLE_RSSD9001)
        match_key = '{}_{}'.format(prefix, const.BRANCH_STATE_NUM)

        data_df.loc[:, match_id] = data_df[match_id].replace({'': np.nan}).dropna().apply(lambda x: str(int(x)))

        # match with rssd9001
        rename_dict1 = {const.COMMERCIAL_RSSD9001: match_id,
                        const.BRANCH_STATE_NUM: match_key}
        tmp_branch_data1 = branch_state_count2.drop([const.COMMERCIAL_RSSD9364], axis=1).rename(
            index=str, columns=rename_dict1)
        data_df_9001 = data_df.merge(tmp_branch_data1, on=[match_id, const.YEAR], how='left')

        # match with rssd9364
        rename_dict2 = {const.COMMERCIAL_RSSD9364: match_id,
                        const.BRANCH_STATE_NUM: match_key}
        tmp_branch_data2 = branch_state_count2.drop([const.COMMERCIAL_RSSD9001], axis=1).dropna(how='any').rename(
            index=str, columns=rename_dict2)
        data_df_9364 = data_df.merge(tmp_branch_data2, on=[match_id, const.YEAR], how='left')

        # merge match result
        data_df_9001.loc[:, match_key] = data_df_9001[match_key].fillna(data_df_9364[match_key])
        data_df = data_df_9001.copy()

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180918_third_part_concise_3018_add_brstatenum.pkl'))
