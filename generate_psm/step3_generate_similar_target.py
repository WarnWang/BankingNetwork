#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step3_generate_similar_target
# @Date: 31/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os
import datetime

import pathos
import pandas as pd
from pscore_match.pscore import PropensityScore

from constants import Constants as const

cov_list = [const.NET_INCOME_LOSS, const.TOTAL_ASSETS, const.TOTAL_LIABILITIES, const.TOTAL_EQUITY_CAPITAL,
            const.ROA, const.ROE, const.LEVERAGE_RATIO, const.NET_INTEREST_INCOME]


def get_psm_index_file(df, match_file, match_type):
    match_file = match_file.dropna(subset=cov_list, how='any').reset_index(drop=True)
    treatment = match_file[const.COMMERCIAL_ID].isin(
        df['{}_{}'.format(match_type, const.LINK_TABLE_RSSD9001)]).apply(int)

    if treatment[treatment == 1].empty:
        use_cov_list = cov_list[:]
        use_cov_list.append(const.COMMERCIAL_ID)
        return pd.DataFrame(columns=use_cov_list)

    covariates = match_file[cov_list]
    pscore = pd.Series(PropensityScore(treatment, covariates).compute('probit'))

    result_df = pd.DataFrame(columns=[const.COMMERCIAL_ID, 0, 1, 2, 3, 4])
    for i in treatment[treatment == 1].index:
        result_dict = {const.COMMERCIAL_ID: match_file.loc[i, const.COMMERCIAL_ID]}
        i_score = pscore[i]
        tmp_series = pscore[pscore.index != i]
        min_dis_index = (tmp_series - i_score).apply(abs).sort_values(ascending=True).head(5).index
        for j, k in enumerate(min_dis_index):
            result_dict[j] = match_file.loc[k, const.COMMERCIAL_ID]
        result_df.loc[result_df.shape[0]] = result_dict

    return result_df


def merge_id_with_link(id_df, rssd9364_data_df, rssd9001_data_df):
    use_cov_list = cov_list[:]
    use_cov_list.append(const.COMMERCIAL_ID)
    rssd9001_tmp_df = rssd9001_data_df[use_cov_list]
    rssd9364_tmp_df = rssd9364_data_df[use_cov_list]

    for i in [const.ACQUIRER, const.TARGET]:
        rename_dict = {}

        for j in use_cov_list:
            rename_dict[j] = '{}_{}'.format(i, j)

        id_rssd_9364 = id_df[id_df['{}_{}'.format(i, const.COMMERCIAL_ID)].apply(int).isin(
            rssd9364_tmp_df[const.COMMERCIAL_ID])]
        id_rssd_9001 = id_df.drop(id_rssd_9364.index, errors='ignore')
        rssd9364_tmp_df2 = rssd9364_tmp_df.rename(index=str, columns=rename_dict)
        id_9364 = id_rssd_9364.merge(rssd9364_tmp_df2, on='{}_{}'.format(i, const.COMMERCIAL_ID), how='left')

        rssd9001_tmp_df2 = rssd9001_tmp_df.rename(index=str, columns=rename_dict)
        id_9001 = id_rssd_9001.merge(rssd9001_tmp_df2, on='{}_{}'.format(i, const.COMMERCIAL_ID), how='left')

        id_df = pd.concat([id_9364, id_9001], axis=0)

    return id_df


def get_pscore_match_year(df):
    year = df[const.YEAR].iloc[0]
    quarter = df[const.QUARTER].iloc[0]
    if year == 2014:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_YEAR_PATH, 'call2013.pkl'))
    else:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_YEAR_PATH, 'call{}.pkl'.format(year)))

    match_file = match_file.dropna(subset=cov_list, how='any').drop_duplicates(subset=[const.COMMERCIAL_RSSD9001],
                                                                               keep='last')

    try:
        # get acquirer index
        rssd9364_match_file = match_file[match_file[const.COMMERCIAL_RSSD9364] > 0].groupby(
            const.COMMERCIAL_RSSD9364)[cov_list].sum().reset_index()
        rssd9364_match_file[const.COMMERCIAL_ID] = rssd9364_match_file[const.COMMERCIAL_RSSD9364]
        matched_result_9364 = get_psm_index_file(df=df, match_file=rssd9364_match_file, match_type=const.ACQUIRER)
        rssd9001_match_file = match_file[match_file[const.COMMERCIAL_RSSD9001] > 0]
        rssd9001_match_file[const.COMMERCIAL_ID] = rssd9001_match_file[const.COMMERCIAL_RSSD9001]
        matched_result_9001 = get_psm_index_file(df=df, match_file=rssd9001_match_file, match_type=const.ACQUIRER)

        acq_matched_result = pd.concat([matched_result_9001, matched_result_9364], axis=0).drop_duplicates(
            subset=[const.COMMERCIAL_ID], keep='first')

        # get target index
        matched_result_9364 = get_psm_index_file(df=df, match_file=rssd9364_match_file, match_type=const.TARGET)
        matched_result_9001 = get_psm_index_file(df=df, match_file=rssd9001_match_file, match_type=const.TARGET)

        tar_matched_result = pd.concat([matched_result_9001, matched_result_9364], axis=0).drop_duplicates(
            subset=[const.COMMERCIAL_ID], keep='first')

    except Exception as err:
        print(err)
        print('{}-{}'.format(year, quarter))
        raise Exception(err)

    columns = []
    for i in [const.ACQUIRER, const.TARGET]:
        for j in [const.REAL, const.COMMERCIAL_ID]:
            columns.append('{}_{}'.format(i, j))

    dfs = []
    for i in df.index:
        temp_df = pd.DataFrame(columns=columns)
        real_acq_id = df.loc[i, '{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001)]
        real_tar_id = df.loc[i, '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)]
        temp_df.loc[temp_df.shape[0]] = {
            '{}_{}'.format(const.ACQUIRER, const.REAL): 1,
            '{}_{}'.format(const.TARGET, const.REAL): 1,
            '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): real_acq_id,
            '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): real_tar_id,
        }
        acq_matched_tmp = acq_matched_result[acq_matched_result[const.COMMERCIAL_ID] == real_acq_id]
        tar_matched_tmp = tar_matched_result[tar_matched_result[const.COMMERCIAL_ID] == real_tar_id]

        if acq_matched_tmp.empty and tar_matched_tmp.empty:
            print('{}-{}'.format(year, quarter))
            raise ValueError('invalid format')

        if not tar_matched_tmp.empty:
            for j in range(5):
                temp_df.loc[temp_df.shape[0]] = {
                    '{}_{}'.format(const.ACQUIRER, const.REAL): 1,
                    '{}_{}'.format(const.TARGET, const.REAL): 0,
                    '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): real_acq_id,
                    '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): tar_matched_tmp[j].iloc[0],
                }

        if not acq_matched_tmp.empty:
            for j in range(5):
                temp_df.loc[temp_df.shape[0]] = {
                    '{}_{}'.format(const.ACQUIRER, const.REAL): 0,
                    '{}_{}'.format(const.TARGET, const.REAL): 1,
                    '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): acq_matched_tmp[j].iloc[0],
                    '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): real_acq_id,
                }

        if not acq_matched_tmp.empty and not tar_matched_tmp.empty:
            for j in range(5):
                for k in range(5):
                    temp_df.loc[temp_df.shape[0]] = {
                        '{}_{}'.format(const.ACQUIRER, const.REAL): 0,
                        '{}_{}'.format(const.TARGET, const.REAL): 0,
                        '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): acq_matched_tmp[j].iloc[0],
                        '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): tar_matched_tmp[k].iloc[0],
                    }

        temp_df.loc[:, '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)] = real_tar_id
        temp_df.loc[:, '{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001)] = real_acq_id
        dfs.append(temp_df)

    generated_index_file = pd.concat(dfs, axis=0, ignore_index=True)
    generated_index_file.loc[:, const.YEAR] = year
    generated_index_file.loc[:, const.QUARTER] = quarter

    generated_index_file.to_pickle(os.path.join(const.TEMP_PATH, '20170831_{}_{}_id_file.pkl'.format(year, quarter)))

    merged_data_df = merge_id_with_link(generated_index_file, rssd9364_data_df=rssd9364_match_file,
                                        rssd9001_data_df=rssd9001_match_file)

    merged_data_df.to_pickle(os.path.join(const.TEMP_PATH, '20170831_{}_{}_data_file.pkl'.format(year, quarter)))
    return merged_data_df


def get_pscore_match(df):
    year = df[const.YEAR].iloc[0]
    quarter = df[const.QUARTER].iloc[0]

    print('{} Start to handle {} - {} data'.format(datetime.datetime.now(), year, quarter))

    if os.path.isfile(os.path.join(const.TEMP_PATH, '20170831_{}_{}_data_file.pkl'.format(year, quarter))):
        print('{}: {} - {} data already have'.format(datetime.datetime.now(), year, quarter))
        return pd.read_pickle(os.path.join(const.TEMP_PATH, '20170831_{}_{}_data_file.pkl'.format(year, quarter)))

    if year >= 2014:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_YEAR_PATH, 'call2013.pkl'))
        match_file = match_file.dropna(subset=cov_list, how='any').drop_duplicates(
            subset=[const.COMMERCIAL_RSSD9001], keep='last')

    elif year in {1985, 1987} and quarter in {3, 4}:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_QUARTER_PATH,
                                                 'call{}06.pkl'.format(year)))

    elif year in {1988} and quarter in {4}:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_QUARTER_PATH,
                                                 'call{}09.pkl'.format(year)))

    elif year in {1991} and quarter in {1}:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_QUARTER_PATH,
                                                 'call199012.pkl'.format(year)))
    elif year == 1986 and quarter in {1, 2, 3}:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_QUARTER_PATH,
                                                 'call{}12.pkl'.format(year)))



    else:
        match_file = pd.read_pickle(os.path.join(const.COMMERCIAL_QUARTER_PATH,
                                                 'call{}{:02d}.pkl'.format(year, quarter * 3)))

    try:
        # get acquirer index
        rssd9364_match_file = match_file[match_file[const.COMMERCIAL_RSSD9364] > 0].groupby(
            const.COMMERCIAL_RSSD9364)[cov_list].sum().reset_index()
        rssd9364_match_file[const.COMMERCIAL_ID] = rssd9364_match_file[const.COMMERCIAL_RSSD9364]
        matched_result_9364 = get_psm_index_file(df=df, match_file=rssd9364_match_file, match_type=const.ACQUIRER)
        rssd9001_match_file = match_file[match_file[const.COMMERCIAL_RSSD9001] > 0]
        rssd9001_match_file[const.COMMERCIAL_ID] = rssd9001_match_file[const.COMMERCIAL_RSSD9001]
        matched_result_9001 = get_psm_index_file(df=df, match_file=rssd9001_match_file, match_type=const.ACQUIRER)

        acq_matched_result = pd.concat([matched_result_9001, matched_result_9364], axis=0).drop_duplicates(
            subset=[const.COMMERCIAL_ID], keep='first')

        # get target index
        matched_result_9364 = get_psm_index_file(df=df, match_file=rssd9364_match_file, match_type=const.TARGET)
        matched_result_9001 = get_psm_index_file(df=df, match_file=rssd9001_match_file, match_type=const.TARGET)

        tar_matched_result = pd.concat([matched_result_9001, matched_result_9364], axis=0).drop_duplicates(
            subset=[const.COMMERCIAL_ID], keep='first')

    except Exception as err:
        print(err)
        print('{}-{}'.format(year, quarter))
        raise Exception(err)

    columns = []
    for i in [const.ACQUIRER, const.TARGET]:
        for j in [const.REAL, const.COMMERCIAL_ID]:
            columns.append('{}_{}'.format(i, j))

    dfs = []
    for i in df.index:
        temp_df = pd.DataFrame(columns=columns)
        real_acq_id = df.loc[i, '{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001)]
        real_tar_id = df.loc[i, '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)]
        temp_df.loc[temp_df.shape[0]] = {
            '{}_{}'.format(const.ACQUIRER, const.REAL): 1,
            '{}_{}'.format(const.TARGET, const.REAL): 1,
            '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): real_acq_id,
            '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): real_tar_id,
        }
        acq_matched_tmp = acq_matched_result[acq_matched_result[const.COMMERCIAL_ID] == real_acq_id]
        tar_matched_tmp = tar_matched_result[tar_matched_result[const.COMMERCIAL_ID] == real_tar_id]

        # if acq_matched_tmp.empty and tar_matched_tmp.empty:
        #     print('{}-{}'.format(year, quarter))
        #     return get_pscore_match_year(df)

        if not tar_matched_tmp.empty:
            for j in range(5):
                temp_df.loc[temp_df.shape[0]] = {
                    '{}_{}'.format(const.ACQUIRER, const.REAL): 1,
                    '{}_{}'.format(const.TARGET, const.REAL): 0,
                    '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): real_acq_id,
                    '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): tar_matched_tmp[j].iloc[0],
                }

        if not acq_matched_tmp.empty:
            for j in range(5):
                temp_df.loc[temp_df.shape[0]] = {
                    '{}_{}'.format(const.ACQUIRER, const.REAL): 0,
                    '{}_{}'.format(const.TARGET, const.REAL): 1,
                    '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): acq_matched_tmp[j].iloc[0],
                    '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): real_acq_id,
                }

        if not acq_matched_tmp.empty and not tar_matched_tmp.empty:
            for j in range(5):
                for k in range(5):
                    temp_df.loc[temp_df.shape[0]] = {
                        '{}_{}'.format(const.ACQUIRER, const.REAL): 0,
                        '{}_{}'.format(const.TARGET, const.REAL): 0,
                        '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID): acq_matched_tmp[j].iloc[0],
                        '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID): tar_matched_tmp[k].iloc[0],
                    }

        temp_df.loc[:, '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)] = real_tar_id
        temp_df.loc[:, '{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001)] = real_acq_id
        dfs.append(temp_df)

    generated_index_file = pd.concat(dfs, axis=0, ignore_index=True)
    generated_index_file.loc[:, const.YEAR] = year
    generated_index_file.loc[:, const.QUARTER] = quarter

    generated_index_file.to_pickle(os.path.join(const.TEMP_PATH, '20170831_{}_{}_id_file.pkl'.format(year, quarter)))

    merged_data_df = merge_id_with_link(generated_index_file, rssd9364_data_df=rssd9364_match_file,
                                        rssd9001_data_df=rssd9001_match_file)

    merged_data_df.to_pickle(os.path.join(const.TEMP_PATH, '20170831_{}_{}_data_file.pkl'.format(year, quarter)))

    print('{}: {} - {} data finished'.format(datetime.datetime.now(), year, quarter))
    return merged_data_df


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(os.path.join(const.TEMP_PATH, '20170831_CAR_useful_col.pkl')))
    groups = data_df.groupby([const.YEAR, const.QUARTER])

    dfs = [tmp_df for _, tmp_df in groups]

    # pool = pathos.multiprocessing.ProcessPool(pathos.multiprocessing.cpu_count() - 2)
    # result_dfs = pool.map(get_pscore_match, dfs)
    # pool.close()
    # pool.join()
    result_dfs = []
    for df in dfs:
        result_dfs.append(get_pscore_match(df))

    result_df = pd.concat(result_dfs, ignore_index=True)
    result_df.to_pickle(os.path.join(const.TEMP_PATH, '20170831_CAR_real_fault_file.pkl'))
