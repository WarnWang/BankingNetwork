#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step10_add_total_loans_related_variables
# @Date: 18/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step10_add_total_loans_related_variables
"""

import os
import multiprocessing
from functools import partial

import numpy as np
import pandas as pd

from constants import Constants as const
from generate_vars_20180910.step9_append_sbl_related_variables import add_delta_change_group_change

ACQ_9001 = '{}_{}'.format(const.ACQ, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TAR, const.LINK_TABLE_RSSD9001)
TOTAL_LOANS = ['BHCP2125', 'BHCK5524', 'BHCK5525', 'BHCK5526', 'BHCK2122', 'BHCP1403', 'BHCP1407']


def generate_2015_2016_data(tmp_df):
    result_df = tmp_df.copy()
    for year in [2014, 2015]:
        tmp_sub_df = tmp_df[tmp_df[const.YEAR] == 2014].copy()
        tmp_sub_df.loc[:, const.YEAR] = year
        result_df = result_df.append(tmp_sub_df, ignore_index=True)

    return result_df


def add_two_total_loan(row):
    acq_total_loan = row['{}_{}'.format(const.ACQ, const.TOTAL_LOAN)]
    tar_total_loan = row['{}_{}'.format(const.TAR, const.TOTAL_LOAN)]
    if np.isnan(acq_total_loan):
        return tar_total_loan

    elif np.isnan(tar_total_loan):
        return acq_total_loan

    return acq_total_loan + tar_total_loan


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180918_third_part_concise_3018_add_acq_tar_sbl.pkl'))
    acq_tar_id_pairs = data_df[[ACQ_9001, TAR_9001, 'Acq_CUSIP', 'Tar_CUSIP']].copy().dropna(
        subset=[ACQ_9001, TAR_9001], how='any').drop_duplicates(subset=['Acq_CUSIP', 'Tar_CUSIP'])

    bhc_save_path = os.path.join(const.DATA_PATH, 'bhc_csv_all_yearly')
    bhcf_data_df = pd.read_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data.pkl'))

    add_chg_to_total_loan = partial(add_delta_change_group_change, data_key=const.TOTAL_LOAN)
    pool = multiprocessing.Pool(38)

    for i, total_loan_key in enumerate(TOTAL_LOANS):
        bhcf_sub_df = pd.DataFrame()

        rename_dict = {}
        for prefix in [const.ACQ, const.TAR, const.ACQ_TAR]:
            for suffix in [const.TOTAL_LOAN, '{}_chg'.format(const.TOTAL_LOAN), '{}_pctchg'.format(const.TOTAL_LOAN)]:
                rename_dict['{}_{}'.format(prefix, suffix)] = '{}_{}_{}'.format(prefix, suffix, i + 1)

        for id_key in [const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364]:
            bhcf_tmp_sub_df = bhcf_data_df[[id_key, const.YEAR, total_loan_key]].dropna(how='any')
            bhcf_tmp_sub_sum_df = bhcf_tmp_sub_df.groupby([id_key, const.YEAR]).sum().reset_index(drop=False)
            bhcf_sub_df = bhcf_sub_df.append(bhcf_tmp_sub_sum_df.rename(
                index=str, columns={id_key: const.COMMERCIAL_RSSD9001}), ignore_index=True)

        bhcf_sub_df = bhcf_sub_df.drop_duplicates(subset=[const.COMMERCIAL_RSSD9001, const.YEAR], keep='first')
        bhcf_sub_df = bhcf_sub_df.rename(index=str, columns={total_loan_key: const.TOTAL_LOAN})

        # prepare for acq or tar merge
        bhcf_sub_group = bhcf_sub_df.groupby([const.COMMERCIAL_RSSD9001, const.YEAR])
        bhcf_sub_dfs = [df for _, df in bhcf_sub_group]
        bhcf_sub_add_chg_dfs = pool.map(add_chg_to_total_loan, bhcf_sub_dfs)
        bhcf_sub_add_chg_df = pd.concat([bhcf_sub_add_chg_dfs], ignore_index=True, sort=False)
        bhcf_sub_add_chg_df_full = generate_2015_2016_data(bhcf_sub_add_chg_df)

        for prefix in [const.TAR, const.ACQ]:
            match_key = '{}_{}'.format(prefix, const.LINK_TABLE_RSSD9001)
            bhcf_rename_dict = {const.COMMERCIAL_RSSD9001: match_key}
            for data_key in bhcf_sub_add_chg_df_full.keys():
                if data_key not in {const.YEAR, const.COMMERCIAL_RSSD9001}:
                    bhcf_rename_dict[data_key] = '{}_{}'.format(prefix, data_key)
            data_to_merge = bhcf_sub_add_chg_df_full.rename(index=str, columns=bhcf_rename_dict)
            data_df = data_df.merge(data_to_merge, on=[match_key, const.YEAR], how='left')

        # prepare for acq tar aggregate match
        acq_total_loan_df = bhcf_sub_df.rename(index=str, columns={const.COMMERCIAL_RSSD9001: ACQ_9001,
                                                                   const.TOTAL_LOAN: '{}_{}'.format(const.ACQ,
                                                                                                    const.TOTAL_LOAN)})
        acq_tl_df = acq_tar_id_pairs.merge(acq_total_loan_df, on=[ACQ_9001], how='left')

        tar_total_loan_df = bhcf_sub_df.rename(index=str, columns={const.COMMERCIAL_RSSD9001: TAR_9001,
                                                                   const.TOTAL_LOAN: '{}_{}'.format(const.TAR,
                                                                                                    const.TOTAL_LOAN)})
        acq_tar_tl_df = acq_tl_df.merge(tar_total_loan_df, on=[TAR_9001, const.YEAR], how='left')

        acq_tar_tl_key = '{}_{}'.format(const.ACQ_TAR, const.TOTAL_LOAN)
        acq_tar_tl_df.loc[:, acq_tar_tl_key] = acq_tar_tl_df.apply(add_two_total_loan, axis=1)
        valid_at_tl_df = acq_tar_tl_df[[ACQ_9001, TAR_9001, const.YEAR, acq_tar_tl_key]].dropna(how='any')

        acq_tar_add_change = partial(add_delta_change_group_change, data_key=acq_tar_tl_key)
        acq_tar_groups = valid_at_tl_df.groupby([ACQ_9001, TAR_9001])
        acq_tar_dfs = [df for _, df in acq_tar_groups]
        acq_tar_chg_dfs = pool.map(acq_tar_add_change, acq_tar_dfs)
        acq_tar_chg_df = pd.concat(acq_tar_chg_dfs, ignore_index=True, sort=False)
        acq_tar_chg_df = generate_2015_2016_data(acq_tar_chg_df)

        data_df = data_df.merge(acq_tar_chg_df, on=[ACQ_9001, TAR_9001, const.YEAR], how='left').drop_duplicates(
            subset=['Acq_CUSIP', 'Tar_CUSIP', const.YEAR])
        data_df = data_df.rename(index=str, columns=rename_dict)

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180918_third_part_concise_3018_add_sbl_tl.pkl'))
    data_df = data_df.replace({np.inf: np.nan})
    data_df.to_stata(os.path.join(const.RESULT_PATH, '20180918_third_part_concise_3018_add_sbl_tl.dta'),
                     write_index=False)
