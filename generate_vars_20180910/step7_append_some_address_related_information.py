#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step7_append_some_address_related_information
# @Date: 13/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step7_append_some_address_related_information
"""

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    data_path = '/home/zigan/Documents/wangyouan/research/BankingNetwork/yuluping/overlap'
    branch_df = pd.read_csv(os.path.join(data_path, '20170816_ross_martin_fdic_76-16.csv')).rename(
        index=str, columns={'RSSDID_RSSD9001': const.COMMERCIAL_RSSD9001, 'YEAR': const.YEAR,
                            'STNUMBR_SUMD9210': const.FIPS_STATE_CODE, 'CNTYNUMB_SUMD9150': const.FIPS_COUNTY_CODE,
                            'RSSDHCR_RSSD9364': const.COMMERCIAL_RSSD9364})

    for key in [const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001, const.FIPS_STATE_CODE, const.FIPS_COUNTY_CODE,
                'ZIPBR_SUMD9220', 'MSABR_SUMD9180']:
        branch_df.loc[:, key] = branch_df[key].dropna().apply(lambda x: str(int(x)))

    branch_df.loc[:, const.FIPS_COUNTY_CODE] = branch_df[const.FIPS_COUNTY_CODE].dropna().apply(lambda x: x.zfill(3))
    branch_df.loc[:, const.FIPS_STATE_CODE] = branch_df[const.FIPS_STATE_CODE].dropna().apply(lambda x: x.zfill(2))
    branch_df.loc[:, 'ZIPBR_SUMD9220'] = branch_df['ZIPBR_SUMD9220'].dropna().apply(lambda x: x.zfill(5))
    branch_df.to_pickle(os.path.join(const.TEMP_PATH, '20180913_ross_martin_fdic_76_16.pkl'))

    headquarter_df = branch_df[branch_df[const.BRANCH_ID_NUM] == 0].copy().drop(['DEPSUMBR_SUMD2200', 'BRNUM_SUMD9021',
                                                                                 'STALPBR'], axis=1)

    # just_branch_df = branch_df[branch_df[const.BRANCH_ID_NUM] != 0].copy()

    branch_state_df = branch_df[[const.FIPS_STATE_CODE, const.YEAR, const.COMMERCIAL_RSSD9001]].drop_duplicates()
    branch_state_count = branch_state_df.groupby([const.YEAR, const.COMMERCIAL_RSSD9001]).count().reset_index(
        drop=False).rename(index=str, columns={const.FIPS_STATE_CODE: const.BRANCH_STATE_NUM})

    fips_county_df = pd.read_csv(os.path.join(const.DATA_PATH, 'fips_county.csv'), dtype=str).drop(
        ['CLASSFP'], axis=1).rename(index=str, columns={'STATEFP': const.FIPS_STATE_CODE,
                                                        'COUNTYFP': const.FIPS_COUNTY_CODE})

    hq_add_br_state_num_df = headquarter_df.merge(branch_state_count, on=[const.YEAR, const.COMMERCIAL_RSSD9001],
                                                  how='left')
    hq_add_br_state_num_df.loc[:, const.BRANCH_STATE_NUM] = hq_add_br_state_num_df[const.BRANCH_STATE_NUM].fillna(1)
    hq_add_cnty_name_df = hq_add_br_state_num_df.merge(fips_county_df,
                                                       on=[const.FIPS_COUNTY_CODE, const.FIPS_STATE_CODE],
                                                       how='left')

    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180913_psm_append_permco.pkl')).drop(['Tar_state_match',
                                                                                                    'Acq_state_match'],
                                                                                                   axis=1)

    for prefix in [const.TARGET, const.ACQUIRER]:
        match_prefix = prefix[:3]
        rename_dict = {const.FIPS_STATE_CODE: '{}_{}'.format(match_prefix, const.STATE_FIPS_MATCH),
                       const.FIPS_COUNTY_CODE: '{}_{}'.format(match_prefix, const.COUNTY_FIPS_MATCH),
                       'MSABR_SUMD9180': '{}_MSA'.format(match_prefix),
                       'ZIPBR_SUMD9220': '{}_{}'.format(match_prefix, const.ZIPCODE_MATCH),
                       'CITYBR': '{}_{}'.format(match_prefix, const.CITY_MATCH),
                       'STATE': '{}_{}'.format(match_prefix, const.STATE_MATCH),
                       'COUNTYNAME': '{}_{}'.format(match_prefix, const.COUNTY_MATCH),
                       const.BRANCH_STATE_NUM: '{}_{}'.format(match_prefix, const.BRANCH_STATE_NUM)
                       }
        hq_match_df = hq_add_cnty_name_df.rename(columns=rename_dict, index=str)

        # first use 9001 to match
        match_key = '{}_{}'.format(prefix, const.COMMERCIAL_ID)
        hq_match_df_9001 = hq_match_df.drop([const.COMMERCIAL_RSSD9364], axis=1).rename(columns={
            const.COMMERCIAL_RSSD9001: match_key}, index=str)
        matched_df_9001 = data_df.merge(hq_match_df_9001, on=[match_key, const.YEAR], how='left')

        # then use 9364 to match
        match_key = '{}_{}'.format(prefix, const.COMMERCIAL_ID)
        hq_match_df_9364 = hq_match_df.drop([const.COMMERCIAL_RSSD9001], axis=1).rename(columns={
            const.COMMERCIAL_RSSD9364: match_key}, index=str)
        matched_df_9364 = data_df.merge(hq_match_df_9364, on=[match_key, const.YEAR], how='left')

        for key in rename_dict.values():
            data_df.loc[:, key] = matched_df_9001[key].fillna(matched_df_9364[key])

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180913_psm_append_address_related_info.pkl'))

    for prefix in [const.TARGET, const.ACQUIRER]:
        key_info = '{}_{}'.format(prefix, const.COMMERCIAL_ID)

        data_df.loc[:, key_info] = data_df[key_info].dropna().apply(int)

    data_df.to_stata(os.path.join(const.RESULT_PATH, '20180913_psm_append_address_related_info.dta'), write_index=False)
    data_df.to_csv(os.path.join(const.RESULT_PATH, '20180913_psm_append_address_related_info.csv'), index=False)
