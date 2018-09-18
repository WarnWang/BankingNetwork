#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step9_append_total_loans
# @Date: 18/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python3 -m generate_vars_20180910.step9_append_total_loans
"""

import os
import multiprocessing
from functools import partial

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


def add_two_sb_loan(row):
    acq_sbl = row['{}_{}'.format(const.ACQ, const.SB_LOAN)]
    tar_sbl = row['{}_{}'.format(const.TAR, const.SB_LOAN)]
    if np.isnan(acq_sbl):
        return tar_sbl

    elif np.isnan(tar_sbl):
        return acq_sbl

    return acq_sbl + tar_sbl


def get_combined_sb_loan(row):
    pass


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180918_third_part_concise_3018_add_brstatenum.pkl'))

    # construct annual RCON5571 data file
    call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')

    call_dfs = []
    date_key_set = {'RCFD9999', 'RSSD9999'}
    for year in range(1993, 2015):
        call_df = pd.read_pickle(os.path.join(call_report_path, 'call{}.pkl'.format(year)))
        current_date_key = list(date_key_set.intersection(call_df.keys()))
        if current_date_key:
            date_key = current_date_key[0]
        else:
            current_date_key = [i for i in call_df.keys() if i.endswith('9999')]
            if current_date_key:
                date_key = current_date_key[0]
            else:
                raise ValueError('No valid key for date in year {}'.format(year))
        call_annual_dfs = []

        for measure_key in [const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364, const.RSSD9379, const.RSSD9360,
                            const.RSSD9348]:
            call_sub_df = call_df[[measure_key, const.SMALL_BUSINESS_LOAN, date_key]].dropna(how='any')
            call_sub_df.loc[:, measure_key] = call_sub_df[measure_key].apply(lambda x: str(int(x)))
            call_sub_agg_df = call_sub_df.groupby([measure_key, date_key]).sum().reset_index(drop=False).drop(
                [date_key], axis=1)
            call_sub_single_df = call_sub_agg_df.groupby(measure_key).max().reset_index(drop=False)
            call_annual_dfs.append(
                call_sub_single_df.rename(index=str, columns={measure_key: const.COMMERCIAL_RSSD9001}))

        merged_call_df = pd.concat(call_annual_dfs, ignore_index=True, sort=False)
        merged_call_df = merged_call_df.drop_duplicates(subset=[const.COMMERCIAL_RSSD9001], keep='last')
        merged_call_df.loc[:, const.YEAR] = year

        call_dfs.append(merged_call_df)

    sbl_df = pd.concat(call_dfs, ignore_index=True, sort=False)
    sbl_df.to_pickle(os.path.join(const.TEMP_PATH, '20180918_small_business_load_93_14.pkl'))

    # construct some loan related_variables
    add_change_to_sbl = partial(add_delta_change_group_change, data_key=const.SMALL_BUSINESS_LOAN)
    sbl_group = sbl_df.groupby([const.COMMERCIAL_RSSD9001])
    sbl_dfs = [df for _, df in sbl_group]
    p = multiprocessing.Pool(38)
    sbl_append_var_dfs = p.map(add_change_to_sbl, sbl_dfs)
    sbl_df_append_sml = pd.concat(sbl_append_var_dfs, ignore_index=True, sort=False)
    sbl_df_append_sml = sbl_df_append_sml.replace({np.inf: np.nan})
    sbl_df_append_sml.to_pickle(os.path.join(const.TEMP_PATH, '20180918_small_business_append_growth_93_14.pkl'))

    # extend variables to year 2016
    call_df_2014 = sbl_df_append_sml[sbl_df_append_sml[const.YEAR] == 2014]
    call_df_2015 = call_df_2014.copy()
    call_df_2016 = call_df_2014.copy()
    call_df_2015.loc[:, const.YEAR] = 2015
    call_df_2016.loc[:, const.YEAR] = 2016
    sbl_df_append_sml = sbl_df_append_sml.append(call_df_2015, ignore_index=True)
    sbl_df_append_sml = sbl_df_append_sml.append(call_df_2016, ignore_index=True)
    sbl_df_append_sml = sbl_df_append_sml[sbl_df_append_sml[const.COMMERCIAL_RSSD9001] != '0']
    sbl_df_append_sml.to_pickle(os.path.join(const.TEMP_PATH, '20180918_small_business_append_growth_93_16.pkl'))

    # prepare branch state number count dataframe
    for prefix in [const.TAR, const.ACQ]:
        match_id = '{}_{}'.format(prefix, const.LINK_TABLE_RSSD9001)

        common_rename_dict = {}
        for key in sbl_df_append_sml.keys():
            if key not in {const.YEAR, const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364}:
                common_rename_dict[key] = '{}_{}'.format(prefix, key)

        # first use rssd 9001
        rename_dict_9001 = common_rename_dict.copy()
        rename_dict_9001[const.COMMERCIAL_RSSD9001] = match_id
        match_data_9001 = sbl_df_append_sml.rename(index=str, columns=rename_dict_9001)
        data_df = data_df.merge(match_data_9001, on=[match_id, const.YEAR], how='left')

    sbl_keys = {i: i.replace(const.SMALL_BUSINESS_LOAN, const.SB_LOAN) for i in data_df.keys() if
                const.SMALL_BUSINESS_LOAN in i}
    data_df = data_df.rename(index=str, columns=sbl_keys)
    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180918_third_part_concise_3018_add_sbl_loan_simple.pkl'))

    pure_sbl_df = sbl_df_append_sml.drop(['RCON5571_chg', 'RCON5571_pctchg'], axis=1).rename(
        index=str, columns={const.SMALL_BUSINESS_LOAN: const.SB_LOAN})
    acq_tar_ids = data_df[[ACQ_9001, TAR_9001, 'Acq_CUSIP', 'Tar_CUSIP']].copy().dropna(
        subset=[ACQ_9001, TAR_9001], how='any').drop_duplicates(subset=['Acq_CUSIP', 'Tar_CUSIP'])
    acq_sml_df = pure_sbl_df.rename(index=str, columns={const.COMMERCIAL_RSSD9001: ACQ_9001,
                                                        const.SB_LOAN: '{}_{}'.format(const.ACQ, const.SB_LOAN)})
    acq_sml_id = acq_tar_ids.merge(acq_sml_df, on=[ACQ_9001], how='left')

    tar_sml_df = pure_sbl_df.rename(index=str, columns={const.COMMERCIAL_RSSD9001: TAR_9001,
                                                        const.SB_LOAN: '{}_{}'.format(const.TAR, const.SB_LOAN)})
    acq_tar_sbl_id = acq_sml_id.merge(tar_sml_df, on=[TAR_9001, const.YEAR], how='left')

    acq_tar_sbl_key = '{}_{}'.format(const.ACQ_TAR, const.SB_LOAN)
    acq_tar_sbl_id.loc[:, acq_tar_sbl_key] = acq_tar_sbl_id.apply(add_two_sb_loan, axis=1)
    valid_at_sbl_df = acq_tar_sbl_id[[ACQ_9001, TAR_9001, const.YEAR, acq_tar_sbl_key]].dropna(how='any')

    acq_tar_add_change = partial(add_delta_change_group_change, data_key=acq_tar_sbl_key)
    acq_tar_groups = valid_at_sbl_df.groupby([ACQ_9001, TAR_9001])
    acq_tar_dfs = [df for _, df in acq_tar_groups]

    acq_tar_chg_dfs = p.map(acq_tar_add_change, acq_tar_dfs)
    acq_tar_chg_df = pd.concat(acq_tar_chg_dfs, ignore_index=True, sort=False)

    data_df_add_acq_tar = data_df.merge(acq_tar_chg_df, on=[ACQ_9001, TAR_9001, const.YEAR], how='left')
    data_df_add_acq_tar = data_df_add_acq_tar.drop_duplicates(subset=['Acq_CUSIP', 'Tar_CUSIP', const.YEAR])
    data_df_add_acq_tar.to_pickle(os.path.join(const.TEMP_PATH, '20180918_third_part_concise_3018_add_acq_tar_sbl.pkl'))
