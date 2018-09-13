#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step6_append_permno_information
# @Date: 13/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step6_append_permno_information
"""

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180912_psm_append_state_match.pkl'))

    permno_df = pd.read_csv(os.path.join(const.DATA_PATH, 'crsp_rssd_link_table_20161231.csv'), dtype=str)
    useful_dataset = permno_df[['entity', 'permco']]

    for prefix in [const.ACQUIRER, const.TARGET]:
        match_key = '{}_{}'.format(prefix, const.COMMERCIAL_ID)

        data_df.loc[:, match_key] = data_df[match_key].apply(str)
        match_df = useful_dataset.rename(index=str, columns={'entity': match_key, 'permco': '{}_permco'.format(prefix)})
        data_df = data_df.merge(match_df, on=match_key, how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180913_psm_append_permco.pkl'))
