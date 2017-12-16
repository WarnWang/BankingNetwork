#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step6_balance_test_result
# @Date: 12/12/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
from scipy.stats import ttest_ind

from constants import Constants as const

tag_list = [const.ACQUIRER, const.TARGET]

test_column = [const.INTEREST_INCOME_RATIO, const.LEVERAGE_RATIO, const.TOTAL_ASSETS, const.NET_INCOME_LOSS, const.ROA]

if __name__ == '__main__':
    df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20171216_merged_psm_data_file.pkl'))

    df = df.rename(index=str, columns={'Acquirer_ROA_y': 'Acquirer_ROA'})

    df_deal_groups = df.groupby('Deal_Number')
    df_num_count = df_deal_groups.count()
    valid_deal_index = df_num_count[df_num_count['Status'] == 36].index

    valid_df = df[df['Deal_Number'].isin(valid_deal_index)]

    value_to_measure = ['mean', 'std', '25', '50', '75', 'min', 'max']

    # valid_target_df = valid_df[valid_df['{}_{}'.format(const.TARGET, const.REAL)] == 1]
    # valid_acquire_df = valid_df[valid_df['{}_{}'.format(const.ACQUIRER, const.REAL)] == 1]

    columns = []
    for i in range(2):
        for j in value_to_measure:
            columns.append('{}_{}'.format(j, i))
    columns.append('t')

    result_df = pd.DataFrame(columns=columns)

    for i in range(2):
        tag = tag_list[1 - i]
        real_tag = tag_list[i]
        tmp_valid_df = valid_df[valid_df['{}_{}'.format(tag, const.REAL)] == 1]
        tmp_real_df = tmp_valid_df[tmp_valid_df['{}_{}'.format(real_tag, const.REAL)] == 1]
        tmp_fake_df = tmp_valid_df[tmp_valid_df['{}_{}'.format(real_tag, const.REAL)] != 1]

        for j in test_column:
            data_key = '{}_{}'.format(real_tag, j)

            result_dict = {'mean_0': tmp_real_df[data_key].mean(),
                           'std_0': tmp_real_df[data_key].std(),
                           '25_0': tmp_real_df[data_key].quantile(q=0.25),
                           '50_0': tmp_real_df[data_key].quantile(q=0.5),
                           '75_0': tmp_real_df[data_key].quantile(q=0.75),
                           'min_0': tmp_real_df[data_key].min(),
                           'max_0': tmp_real_df[data_key].max(),
                           'mean_1': tmp_fake_df[data_key].mean(),
                           'std_1': tmp_fake_df[data_key].std(),
                           '25_1': tmp_fake_df[data_key].quantile(q=0.25),
                           '50_1': tmp_fake_df[data_key].quantile(q=0.5),
                           '75_1': tmp_fake_df[data_key].quantile(q=0.75),
                           'min_1': tmp_fake_df[data_key].min(),
                           'max_1': tmp_fake_df[data_key].max(),
                           't': ttest_ind(tmp_real_df[data_key], tmp_fake_df[data_key])[0]
                           }
            result_df.loc[data_key] = result_dict

    result_df.to_excel(os.path.join(const.RESULT_PATH, '20171216_balance_test.xlsx'))
