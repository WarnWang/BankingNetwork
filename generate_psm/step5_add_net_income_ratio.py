#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step5_add_net_income_ratio
# @Date: 12/12/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    for root_dir in [const.COMMERCIAL_QUARTER_PATH, const.COMMERCIAL_YEAR_PATH]:
        for f in os.listdir(root_dir):
            df = pd.read_pickle(os.path.join(root_dir, f))
            df[const.INTEREST_INCOME_RATIO] = df[const.NET_INTEREST_INCOME] / df[const.TOTAL_ASSETS]

            df.to_pickle(os.path.join(root_dir, f))
