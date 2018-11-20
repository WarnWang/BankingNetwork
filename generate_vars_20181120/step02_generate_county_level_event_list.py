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

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const


def add_fips_to_county_event_df(county_event_df, branch_info, event_year, event_id):
    for fips in branch_info[const.FIPS]:
        sub_county_event_df = county_event_df[(county_event_df[const.FIPS] == fips)
                                              & (county_event_df[const.YEAR] == event_year)]
        if sub_county_event_df.empty:
            county_event_df = county_event_df.append({const.FIPS: fips, const.YEAR: event_year,
                                                      'Deal_Number': str(event_id)},
                                                     ignore_index=True)

        else:
            current_deal = set(sub_county_event_df.iloc[0]['Deal_Number'].split('|'))
            current_deal.add(str(event_id))
            new_deal = '|'.join(list(current_deal))
            county_event_df.loc[sub_county_event_df.index[0], 'Deal_Number'] = new_deal

    return county_event_df


if __name__ == '__main__':
    data_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_fips.pkl'))
    branch_bank_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_branch_location_total_deposit_info.pkl'))
    branch_bank_df.loc[:, const.FIPS] = branch_bank_df[const.FIPS].apply(int)
    branch_bank_df.loc[:, const.RSSD9001] = branch_bank_df[const.RSSD9001].apply(lambda x: str(int(x)))

    common_county_event = pd.DataFrame(columns=[const.FIPS, const.YEAR, 'Deal_Number'])
    tar_county_event = pd.DataFrame(columns=[const.FIPS, const.YEAR, 'Deal_Number'])
    acq_county_event = pd.DataFrame(columns=[const.FIPS, const.YEAR, 'Deal_Number'])

    for i in data_df.index:
        acq_9001 = data_df.loc[i, const.ACQ_9001]
        tar_9001 = data_df.loc[i, const.TAR_9001]
        event_deal_id = data_df.loc[i, 'Deal_Number']
        year = data_df.loc[i, const.YEAR_MERGE]

        current_branch_df = branch_bank_df[branch_bank_df['year'] == year]

        acq_branch = current_branch_df[current_branch_df[const.RSSD9001] == acq_9001]
        tar_branch = current_branch_df[current_branch_df[const.RSSD9001] == tar_9001]

        if acq_branch.empty and tar_branch.empty:
            continue

        if not acq_branch.empty:
            acq_county_event = add_fips_to_county_event_df(county_event_df=acq_county_event, branch_info=acq_branch,
                                                           event_id=event_deal_id, event_year=year)

        if not tar_branch.empty:
            tar_county_event = add_fips_to_county_event_df(county_event_df=tar_county_event, branch_info=tar_branch,
                                                           event_id=event_deal_id, event_year=year)

        if not (tar_branch.empty or acq_branch.empty):
            co_exist_county = acq_branch[acq_branch[const.FIPS].isin(set(tar_branch[const.FIPS]))]
            common_county_event = add_fips_to_county_event_df(county_event_df=common_county_event,
                                                              branch_info=co_exist_county,
                                                              event_id=event_deal_id, event_year=year)


    def get_exclude_rssd9001_list(county_event_df, event_data_df, prefix):
        if prefix == const.TAR:
            used_rssd = [const.TAR_9001]

        elif prefix == const.ACQ:
            used_rssd = [const.ACQ_9001]

        else:
            used_rssd = [const.TAR_9001, const.ACQ_9001]

        county_event_df.loc[:, const.RSSD9001] = np.nan

        for j in county_event_df.index:
            event_id_set = set(map(int, county_event_df.loc[j, 'Deal_Number'].split('|')))
            vaild_event_df = event_data_df[event_data_df['Deal_Number'].isin(event_id_set)]

            rssd_set = set()
            for rssd in used_rssd:
                rssd_set.add(vaild_event_df[rssd])

            county_event_df.loc[j, const.RSSD9001] = '|'.join(list(rssd_set))

        return county_event_df


    acq_county_event_rssd = get_exclude_rssd9001_list(county_event_df=acq_county_event, event_data_df=data_df,
                                                      prefix=const.ACQ)
    tar_county_event_rssd = get_exclude_rssd9001_list(county_event_df=tar_county_event, event_data_df=data_df,
                                                      prefix=const.TAR)
    common_county_event_rssd = get_exclude_rssd9001_list(county_event_df=common_county_event, event_data_df=data_df,
                                                         prefix=const.TAR)

    acq_county_event_rssd.to_pickle(os.path.join(const.TEMP_PATH, '20181120_acq_county_event_rssd.pkl'))
    tar_county_event_rssd.to_pickle(os.path.join(const.TEMP_PATH, '20181120_tar_county_event_rssd.pkl'))
    common_county_event_rssd.to_pickle(os.path.join(const.TEMP_PATH, '20181120_common_county_event_rssd.pkl'))
