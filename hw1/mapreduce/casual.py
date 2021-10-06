#!/usr/bin/env python3

import pandas as pd
import sklearn

filename = "/mapreduce/AB_NYC_2019.csv"

price = pd.read_csv(filename)["price"]

print('{}\t{}'.format(price.mean(), price.var(ddof=0)))