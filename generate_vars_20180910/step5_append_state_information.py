#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step5_append_state_information
# @Date: 12/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step5_append_state_information
"""

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    data_df = pd.read_stata(os.path.join(const.DATA_PATH, '20180911_psm_append_new_variables.dta'))
    for i in [const.TARGET, const.ACQUIRER]:
        data_df.loc[:, '{}_{}'.format(i, const.COMMERCIAL_ID)] = data_df[
            '{}_{}'.format(i, const.COMMERCIAL_ID)].apply(str)

    bank_branch_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_bank_branch_info.pkl'))
    hq_fips_df = bank_branch_df[bank_branch_df[const.BRANCH_ID_NUM] == 0].copy().drop_duplicates(
        subset=[const.COMMERCIAL_RSSD9001, const.YEAR])
    hq_fips_df.loc[:, const.FIPS_STATE_CODE] = hq_fips_df[const.FIPS].apply(lambda x: x[:2])

    state_fips_df = pd.read_csv(os.path.join(const.DATA_PATH, 'state_fips.csv'), sep='|',
                                usecols=['STATE', 'STUSAB'], dtype=str).rename(index=str, columns={
        'STATE': const.FIPS_STATE_CODE, 'STUSAB': const.STATE_MATCH
    })
    hq_fips_merged_df = hq_fips_df.merge(state_fips_df, on=[const.FIPS_STATE_CODE], how='left')
    valid_hq_state_df = hq_fips_merged_df.drop([const.FIPS_STATE_CODE, const.FIPS, const.BRANCH_ID_NUM], axis=1)

    for tag in [const.TARGET, const.ACQUIRER]:
        new_key = '{}_{}'.format(tag[:3], const.STATE_MATCH)
        tmp_hq_df = valid_hq_state_df.rename(index=str, columns={const.STATE_MATCH: new_key})

        # use rssd 9001 to match
        match_key = '{}_{}'.format(tag, const.COMMERCIAL_ID)
        tmp_hq_df_9001 = tmp_hq_df.drop([const.COMMERCIAL_RSSD9364], axis=1).rename(
            index=str, columns={const.COMMERCIAL_RSSD9001: match_key}
        )
        data_df_9001 = data_df.merge(tmp_hq_df_9001, on=[match_key, const.YEAR], how='left')

        tmp_hq_df_9364 = tmp_hq_df.drop([const.COMMERCIAL_RSSD9001], axis=1).rename(
            index=str, columns={const.COMMERCIAL_RSSD9364: match_key}
        ).dropna(how='any')
        data_df_9364 = data_df.merge(tmp_hq_df_9364, on=[match_key, const.YEAR], how='left')

        data_df.loc[:, new_key] = data_df_9001[new_key].fillna(data_df_9364[new_key])

    for i in [const.TARGET, const.ACQUIRER]:
        data_df.loc[:, '{}_{}'.format(i, const.COMMERCIAL_ID)] = data_df[
            '{}_{}'.format(i, const.COMMERCIAL_ID)].apply(int)

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180912_psm_append_state_match.pkl'))
    data_df.to_stata(os.path.join(const.RESULT_PATH, '20180912_psm_append_state_match.dta'), write_index=False)
