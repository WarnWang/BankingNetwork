#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step9_generate_psm_data
# @Date: 10/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_psm.step9_generate_psm_data
"""

import os
import datetime

import numpy as np
import pandas as pd
from pscore_match.pscore import PropensityScore

from constants import Constants as const

PSM_COV_LIST = [const.INTEREST_INCOME_RATIO, const.TOTAL_ASSETS, const.NET_INCOME_LOSS, const.LEVERAGE_RATIO, const.ROA,
                const.MORTGAGE_LENDING_RATIO, const.BANK_TYPE, const.BANK_EFFICIENCY, const.BUSINESS_FOCUS,
                const.SBL_RATIO, const.MORTGAGE_LENDING_RATIO]

DATE_STRING = '20180910'
TMP_SAVE_PATH = os.path.join(const.TEMP_PATH, '{}_temp_result'.format(DATE_STRING))
if not os.path.isdir(TMP_SAVE_PATH):
    os.makedirs(TMP_SAVE_PATH)


def generate_rssd9364_data(rssd9364_group, cov_list):
    sum_df = rssd9364_group.sum().reset_index(drop=False)
    mean_df = rssd9364_group.mean().reset_index(drop=False)

    for key in cov_list:
        if key == const.BANK_TYPE:
            sum_df.loc[:, key] = mean_df[key]

        elif key == const.INTEREST_INCOME_RATIO:
            sum_df.loc[:, key] = sum_df[const.NET_INTEREST_INCOME] / sum_df[const.TOTAL_ASSETS]

        elif key == const.ROA:
            sum_df.loc[:, key] = sum_df[const.NET_INCOME_LOSS] / sum_df[const.TOTAL_ASSETS]

        elif key == const.MORTGAGE_LENDING_RATIO:
            sum_df.loc[:, key] = sum_df[const.MORTGAGE_LENDING] / sum_df[const.TOTAL_ASSETS]

        elif key == const.SBL_RATIO:
            sum_df.loc[:, key] = sum_df[const.SMALL_BUSINESS_LENDING] / sum_df[const.TOTAL_ASSETS]

        elif key == const.BANK_EFFICIENCY:
            sum_df.loc[:, key] = sum_df[const.TOTAL_LOANS] / sum_df[const.EMPLOYEE_NUMBER]

        elif key == const.BUSINESS_FOCUS:
            sum_df.loc[:, key] = sum_df[const.TOTAL_DEPOSITS] / sum_df[const.TOTAL_ASSETS]

        elif key == const.LEVERAGE_RATIO:
            sum_df.loc[:, key] = sum_df[const.TOTAL_LIABILITIES] / sum_df[const.TOTAL_ASSETS]

    return sum_df


def get_psm_index_file(df, match_file, match_type, cov_list=None):
    if cov_list is None:
        cov_list = PSM_COV_LIST

    match_file = match_file.replace({float('inf'): np.nan}).dropna(subset=cov_list, how='any').reset_index(drop=True)
    treatment = match_file[const.COMMERCIAL_ID].isin(
        set(df['{}_{}'.format(match_type, const.LINK_TABLE_RSSD9001)])).apply(int)

    if treatment[treatment == 1].empty:
        use_cov_list = cov_list[:]
        use_cov_list.append(const.COMMERCIAL_ID)
        return pd.DataFrame(columns=use_cov_list)

    covariates = match_file[cov_list]
    pscore = pd.Series(PropensityScore(treatment, covariates).compute('probit'))
    columns = [const.COMMERCIAL_ID, 'pscore']
    for i in range(5):
        columns.append(str(i))
        columns.append('{}_score'.format(i))

    result_df = pd.DataFrame(columns=columns)
    for i in treatment[treatment == 1].index:
        i_score = pscore[i]
        result_dict = {const.COMMERCIAL_ID: str(int(match_file.loc[i, const.COMMERCIAL_ID])),
                       'pscore': i_score}
        tmp_series = pscore[pscore.index != i]
        min_dis_index = (tmp_series - i_score).apply(abs).sort_values(ascending=True).head(5).index
        for j, k in enumerate(min_dis_index):
            result_dict[str(j)] = str(int(match_file.loc[k, const.COMMERCIAL_ID]))
            result_dict['{}_score'.format(j)] = pscore.loc[k]
        result_df.loc[result_df.shape[0]] = result_dict

    return result_df


def merge_id_with_link(id_df, rssd9364_data_df, rssd9001_data_df, cov_list):
    use_cov_list = cov_list[:]
    use_cov_list.append(const.COMMERCIAL_ID)
    rssd9001_tmp_df = rssd9001_data_df[use_cov_list]
    rssd9364_tmp_df = rssd9364_data_df[use_cov_list]
    rssd9001_tmp_df.loc[:, const.COMMERCIAL_ID] = rssd9001_tmp_df[const.COMMERCIAL_ID].apply(lambda x: str(int(x)))
    rssd9364_tmp_df.loc[:, const.COMMERCIAL_ID] = rssd9364_tmp_df[const.COMMERCIAL_ID].apply(lambda x: str(int(x)))

    for i in [const.ACQUIRER, const.TARGET]:
        rename_dict = {}

        for j in use_cov_list:
            rename_dict[j] = '{}_{}'.format(i, j)

        id_key = '{}_{}'.format(i, const.COMMERCIAL_ID)
        id_df.loc[:, id_key] = id_df[id_key].apply(lambda x: str(int(x)))
        id_rssd_9364 = id_df[id_df[id_key].isin(set(rssd9364_tmp_df[const.COMMERCIAL_ID]))]
        id_rssd_9001 = id_df.drop(id_rssd_9364.index, errors='ignore')
        rssd9364_tmp_df2 = rssd9364_tmp_df.rename(index=str, columns=rename_dict)
        id_9364 = id_rssd_9364.merge(rssd9364_tmp_df2, on=['{}_{}'.format(i, const.COMMERCIAL_ID)], how='left')

        rssd9001_tmp_df2 = rssd9001_tmp_df.rename(index=str, columns=rename_dict)
        id_9001 = id_rssd_9001.merge(rssd9001_tmp_df2, on=['{}_{}'.format(i, const.COMMERCIAL_ID)], how='left')

        id_df = pd.concat([id_9364, id_9001], axis=0, ignore_index=True, sort=False)

    return id_df


def get_pscore_match(df_to_match):
    year = df_to_match[const.YEAR].iloc[0]
    quarter = df_to_match[const.QUARTER].iloc[0]

    tmp_save_file_path = os.path.join(TMP_SAVE_PATH, '{}_{}_data_file.pkl'.format(year, quarter))

    # print('{} Start to handle {} - {} data'.format(datetime.datetime.now(), year, quarter))

    if os.path.isfile(tmp_save_file_path):
        # print('{}: {} - {} data already have'.format(datetime.datetime.now(), year, quarter))
        return pd.read_pickle(tmp_save_file_path)

    useful_col_list = PSM_COV_LIST[:]
    if year >= 2014:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_YEAR_PATH, 'call2014.pkl'))

    else:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_QUARTER_PATH,
                                                 'call{}{:02d}.pkl'.format(year, quarter * 3)))

    useful_col_list = list(set(useful_col_list).intersection(set(match_file.keys())))
    match_file = match_file.dropna(subset=useful_col_list, how='any').drop_duplicates(
        subset=[const.COMMERCIAL_RSSD9001], keep='last')

    try:
        # get acquirer index
        rssd9364_group = match_file[match_file[const.COMMERCIAL_RSSD9364] > 0].groupby(
            const.COMMERCIAL_RSSD9364)
        rssd9364_sum_df = generate_rssd9364_data(rssd9364_group, useful_col_list)

        rssd9364_match_file = rssd9364_sum_df.copy()
        rssd9364_match_file.loc[:, const.COMMERCIAL_ID] = rssd9364_match_file[const.COMMERCIAL_RSSD9364].apply(
            lambda x: str(int(x))
        )
        matched_result_9364 = get_psm_index_file(df=df_to_match, match_file=rssd9364_match_file,
                                                 match_type=const.ACQUIRER, cov_list=useful_col_list)
        rssd9001_match_file = match_file[match_file[const.COMMERCIAL_RSSD9001] > 0]
        rssd9001_match_file.loc[:, const.COMMERCIAL_ID] = rssd9001_match_file[const.COMMERCIAL_RSSD9001].apply(
            lambda x: str(int(x))
        )
        matched_result_9001 = get_psm_index_file(df=df_to_match, match_file=rssd9001_match_file,
                                                 match_type=const.ACQUIRER, cov_list=useful_col_list)

        acq_matched_result = pd.concat([matched_result_9001, matched_result_9364], axis=0, sort=False).drop_duplicates(
            subset=[const.COMMERCIAL_ID], keep='first')

        # get target index
        matched_result_9364 = get_psm_index_file(df=df_to_match, match_file=rssd9364_match_file,
                                                 match_type=const.TARGET, cov_list=useful_col_list)
        matched_result_9001 = get_psm_index_file(df=df_to_match, match_file=rssd9001_match_file,
                                                 match_type=const.TARGET, cov_list=useful_col_list)

        tar_matched_result = pd.concat([matched_result_9001, matched_result_9364], axis=0, sort=False).drop_duplicates(
            subset=[const.COMMERCIAL_ID], keep='first')

    except Exception as err:
        print(err)
        print('{}-{}'.format(year, quarter))
        raise Exception(err)

    columns = [const.ACQ_PSCORE, const.ACQ_PSCORE_RANK, const.TAR_PSCORE, const.TAR_PSCORE_RANK]
    for i in [const.ACQUIRER, const.TARGET]:
        for j in [const.REAL, const.COMMERCIAL_ID]:
            columns.append('{}_{}'.format(i, j))

    dfs = []
    for i in df_to_match.index:
        temp_df = pd.DataFrame(columns=columns)
        real_acq_id = int(df_to_match.loc[i, '{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001)])
        real_tar_id = int(df_to_match.loc[i, '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)])
        index = 0
        temp_df.loc[index] = {
            '{}_{}'.format(const.ACQUIRER, const.REAL): 1,
            '{}_{}'.format(const.TARGET, const.REAL): 1,
            '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): real_acq_id,
            '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): real_tar_id,
            const.TAR_PSCORE_RANK: 0,
            const.ACQ_PSCORE_RANK: 0,
            const.ACQ_PSCORE: np.nan,
            const.TAR_PSCORE: np.nan,
        }
        index += 1
        acq_matched_tmp = acq_matched_result[acq_matched_result[const.COMMERCIAL_ID] == real_acq_id]
        tar_matched_tmp = tar_matched_result[tar_matched_result[const.COMMERCIAL_ID] == real_tar_id]

        if not tar_matched_tmp.empty:
            for j in range(5):
                temp_df.loc[index] = {
                    '{}_{}'.format(const.ACQUIRER, const.REAL): 1,
                    '{}_{}'.format(const.TARGET, const.REAL): 0,
                    '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): real_acq_id,
                    '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): tar_matched_tmp[str(j)].iloc[0],
                    const.TAR_PSCORE_RANK: j + 1,
                    const.ACQ_PSCORE_RANK: 0,
                    const.ACQ_PSCORE: np.nan,
                    const.TAR_PSCORE: tar_matched_tmp.loc[tar_matched_tmp.index[0], '{}_score'.format(j)],
                }
                index += 1

        if not acq_matched_tmp.empty:
            for j in range(5):
                temp_df.loc[index] = {
                    '{}_{}'.format(const.ACQUIRER, const.REAL): 0,
                    '{}_{}'.format(const.TARGET, const.REAL): 1,
                    '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): acq_matched_tmp[str(j)].iloc[0],
                    '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): real_acq_id,
                    const.ACQ_PSCORE_RANK: j + 1,
                    const.TAR_PSCORE_RANK: 0,
                    const.TAR_PSCORE: np.nan,
                    const.ACQ_PSCORE: acq_matched_tmp.loc[acq_matched_tmp.index[0], '{}_score'.format(j)],
                }
                index += 1

        if not acq_matched_tmp.empty and not tar_matched_tmp.empty:
            for j in range(5):
                for k in range(5):
                    temp_df.loc[index] = {
                        '{}_{}'.format(const.ACQUIRER, const.REAL): 0,
                        '{}_{}'.format(const.TARGET, const.REAL): 0,
                        '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): acq_matched_tmp[str(j)].iloc[0],
                        '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): tar_matched_tmp[str(k)].iloc[0],
                        const.ACQ_PSCORE_RANK: j + 1,
                        const.TAR_PSCORE_RANK: k + 1,
                        const.TAR_PSCORE: tar_matched_tmp.loc[tar_matched_tmp.index[0], '{}_score'.format(k)],
                        const.ACQ_PSCORE: acq_matched_tmp.loc[acq_matched_tmp.index[0], '{}_score'.format(j)],
                    }
                    index += 1

        if not acq_matched_tmp.empty:
            temp_df[temp_df['{}_{}'.format(const.ACQUIRER, const.REAL)] == 1][const.ACQ_PSCORE] = \
                acq_matched_tmp['pscore'].iloc[0]

        if not tar_matched_tmp.empty:
            temp_df[temp_df['{}_{}'.format(const.TARGET, const.REAL)] == 1][const.TAR_PSCORE] = \
                tar_matched_tmp['pscore'].iloc[0]

        temp_df.loc[:, '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)] = real_tar_id
        temp_df.loc[:, '{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001)] = real_acq_id
        dfs.append(temp_df)

    generated_index_file = pd.concat(dfs, axis=0, ignore_index=True, sort=False)
    generated_index_file.loc[:, const.YEAR] = year
    generated_index_file.loc[:, const.QUARTER] = quarter

    generated_index_file.to_pickle(os.path.join(TMP_SAVE_PATH, '{}_{}_id_file.pkl'.format(year, quarter)))

    merged_data_df = merge_id_with_link(generated_index_file, rssd9364_data_df=rssd9364_match_file,
                                        rssd9001_data_df=rssd9001_match_file, cov_list=useful_col_list)

    merged_data_df.to_pickle(tmp_save_file_path)

    print('{}: {} - {} data finished'.format(datetime.datetime.now(), year, quarter))
    return merged_data_df


if __name__ == '__main__':
    psm_data = pd.read_stata(os.path.join(const.DATA_PATH, '20180908_revision', '20180908_psm_add_missing_rssd.dta'))
    real_psm_data = psm_data[(psm_data['Target_real'] == 1) & (psm_data['Acquirer_real'] == 1)]

    psm_group = real_psm_data.groupby([const.QUARTER, const.YEAR])

    result_dfs = []
    for key, sub_df in psm_group:
        print('{} Start to handle {} - {} data'.format(datetime.datetime.now(), key[0], key[1]))
        result_dfs.append(get_pscore_match(sub_df))

    result_df = pd.concat(result_dfs, ignore_index=True, sort=False)
    result_df.drop_duplicates().to_pickle(
        os.path.join(const.TEMP_PATH, '{}_CAR_real_fault_file.pkl'.format(DATE_STRING)))
