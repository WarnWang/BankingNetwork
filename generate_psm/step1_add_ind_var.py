#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step1_add_ind_var
# @Date: 31/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os
import datetime

import pandas as pd

from constants import Constants as const

COMMERCIAL_ROOT_PATH = '/home/zigan/Documents/yuluping/research/bankingnetwork/Chicago_Fed_data/WRDS ssh data'
COMMERCIAL_QUARTER_PATH = os.path.join(COMMERCIAL_ROOT_PATH, 'commercial_csv')
COMMERCIAL_YEAR_PATH = os.path.join(COMMERCIAL_ROOT_PATH, 'commercial_csv_yearly')

if not os.path.isdir(const.COMMERCIAL_QUARTER_PATH):
    os.makedirs(const.COMMERCIAL_QUARTER_PATH)

if not os.path.isdir(const.COMMERCIAL_YEAR_PATH):
    os.makedirs(const.COMMERCIAL_YEAR_PATH)

for root_dir, out_put_dir in [(COMMERCIAL_QUARTER_PATH, const.COMMERCIAL_QUARTER_PATH),
                              (COMMERCIAL_YEAR_PATH, const.COMMERCIAL_YEAR_PATH)]:
    quarter_files = os.listdir(root_dir)

    for f in quarter_files:
        print(f)
        f_name = os.path.splitext(f)[0]
        save_path = os.path.join(out_put_dir, '{}.pkl'.format(f_name))

        df = pd.read_csv(os.path.join(root_dir, f))
        # df[const.COMMERCIAL_DATE] = df[const.COMMERCIAL_DATE].dropna().apply(
        #     lambda x: datetime.datetime.strptime(str(int(x)), '%Y%m%d'))
        df[const.COMMERCIAL_RSSD9364] = pd.to_numeric(df[const.COMMERCIAL_RSSD9364], errors='coerce', downcast='integer')
        df[const.COMMERCIAL_RSSD9001] = pd.to_numeric(df[const.COMMERCIAL_RSSD9001], errors='coerce', downcast='integer')
        date_info = f_name[4:]
        df.loc[:, const.COMMERCIAL_DATE] = date_info

        df[const.LEVERAGE_RATIO] = df[const.TOTAL_LIABILITIES] / df[const.TOTAL_ASSETS]
        df[const.INTEREST_INCOME_RATIO] = df[const.NET_INTEREST_INCOME] / df[const.TOTAL_ASSETS]
        df[const.ROA] = df[const.NET_INCOME_LOSS] / df[const.TOTAL_ASSETS]
        if const.TOTAL_EQUITY_CAPITAL in df.keys():
            df[const.ROE] = df[const.NET_INCOME_LOSS] / df[const.TOTAL_EQUITY_CAPITAL]

        df.to_pickle(save_path)
