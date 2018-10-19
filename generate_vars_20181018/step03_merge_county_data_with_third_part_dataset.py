#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step03_merge_county_data_with_third_part_dataset
# @Date: 19/10/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181018.step03_merge_county_data_with_third_part_dataset
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

ACQ_9001 = '{}_{}'.format(const.ACQ, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TAR, const.LINK_TABLE_RSSD9001)

if __name__ == '__main__':
    data_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_fips.pkl'))
    county_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181018_county_level_data_rotated_data.pkl'))

    for tag in [const.TAR, const.ACQ]:
        rename_dict = {key: '{}_{}'.format(tag, key) for key in county_df.keys() if key != const.YEAR}
        county_df_rename: DataFrame = county_df.rename(index=str, columns=rename_dict)
        data_df = data_df.merge(county_df_rename, on=[const.YEAR, '{}_{}'.format(tag, const.FIPS)], how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20181019_third_part_concise_3018_append_county_data.pkl'))
    data_df.to_stata(os.path.join(const.RESULT_PATH, '20181019_third_part_concise_3018_append_county_data.dta'),
                     write_index=False)
