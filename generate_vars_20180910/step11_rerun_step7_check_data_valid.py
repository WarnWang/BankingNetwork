#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step11_rerun_step7_check_data_valid
# @Date: 18/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step11_rerun_step7_check_data_valid
"""

import os
import multiprocessing

import pandas as pd

from constants import Constants as const


def fill_all_df(sub_df):
    min_year = sub_df[const.YEAR].min()
    max_year = sub_df[const.YEAR].max()

    year_set = set(range(min_year, max_year + 1))
    not_in_set = year_set.difference(sub_df[const.YEAR])
    for year in not_in_set:
        sub_df = sub_df.append({const.YEAR: year}, ignore_index=True)

    result_df = sub_df.sort_values(by=const.YEAR, ascending=True).fillna(method='ffill')
    return result_df


if __name__ == '__main__':
    branch_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180913_ross_martin_fdic_76_16.pkl'))

    headquarter_df = branch_df[branch_df[const.BRANCH_ID_NUM] == 0].copy().drop(
        ['DEPSUMBR_SUMD2200', 'BRNUM_SUMD9021', 'STALPBR', const.COMMERCIAL_RSSD9364], axis=1)

    bhc_save_path = os.path.join(const.DATA_PATH, 'bhc_csv_all_yearly')
    bhcf_data_df = pd.read_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data.pkl'))
    bhcf_data_df_useful = bhcf_data_df[[const.COMMERCIAL_RSSD9001, const.YEAR, 'RSSD9150', 'RSSD9210', 'RSSD9220',
                                        'RSSD9130']].rename(index=str, columns={'RSSD9150': 'sumd9150',
                                                                                'RSSD9210': 'sumd9210',
                                                                                'RSSD9220': 'ZIPBR_SUMD9220',
                                                                                'RSSD9130': 'CITYBR'})
    format_str = (lambda x: x if isinstance(x, str) else str(int(x)))
    for key in [const.FIPS_STATE_CODE, const.FIPS_COUNTY_CODE, 'ZIPBR_SUMD9220', const.COMMERCIAL_RSSD9001]:
        bhcf_data_df_useful.loc[:, key] = bhcf_data_df_useful[key].dropna().apply(format_str)
    merged_headquarter = headquarter_df.append(bhcf_data_df_useful, ignore_index=True, sort=False).drop_duplicates(
        subset=[const.COMMERCIAL_RSSD9001, const.YEAR], keep='last')
    merged_headquarter_group = merged_headquarter.groupby([const.COMMERCIAL_RSSD9001, const.YEAR])
    merged_headquarter_dfs = [df for _, df in merged_headquarter_group]
    pool = multiprocessing.Pool(38)
    fillna_hq_dfs = pool.map(fill_all_df, merged_headquarter_dfs)
    hq_df_fillna = pd.concat(fillna_hq_dfs, ignore_index=True, sort=False)

    fips_county_df = pd.read_csv(os.path.join(const.DATA_PATH, 'fips_county.csv'), dtype=str).drop(
        ['CLASSFP'], axis=1).rename(index=str, columns={'STATEFP': const.FIPS_STATE_CODE,
                                                        'COUNTYFP': const.FIPS_COUNTY_CODE})
    hq_add_cnty_name_df = hq_df_fillna.merge(fips_county_df, on=[const.FIPS_COUNTY_CODE, const.FIPS_STATE_CODE],
                                             how='left')

    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180913_psm_append_permco.pkl')).drop(
        ['Tar_state_match', 'Acq_state_match'], axis=1)

    for prefix in [const.TARGET, const.ACQUIRER]:
        match_prefix = prefix[:3]
        rename_dict = {const.FIPS_STATE_CODE: '{}_{}'.format(match_prefix, const.STATE_FIPS_MATCH),
                       const.FIPS_COUNTY_CODE: '{}_{}'.format(match_prefix, const.COUNTY_FIPS_MATCH),
                       'MSABR_SUMD9180': '{}_MSA'.format(match_prefix),
                       'ZIPBR_SUMD9220': '{}_{}'.format(match_prefix, const.ZIPCODE_MATCH),
                       'CITYBR': '{}_{}'.format(match_prefix, const.CITY_MATCH),
                       'STATE': '{}_{}'.format(match_prefix, const.STATE_MATCH),
                       'COUNTYNAME': '{}_{}'.format(match_prefix, const.COUNTY_MATCH),
                       }
        hq_match_df = hq_add_cnty_name_df.rename(columns=rename_dict, index=str)

        # first use 9001 to match
        match_key = '{}_{}'.format(prefix, const.COMMERCIAL_ID)
        hq_match_df_9001 = hq_match_df.rename(columns={const.COMMERCIAL_RSSD9001: match_key}, index=str)
        data_df = data_df.merge(hq_match_df_9001, on=[match_key, const.YEAR], how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180919_psm_append_address_related_info.pkl'))
    data_df.to_csv(os.path.join(const.RESULT_PATH, '20180919_psm_append_address_related_info.csv'), index=False)
