#!/usr/bin/env python3

import sys
import math


def calc_mean(count_left, mean_left, count_right, mean_right):
    mean = (count_left * mean_left + count_right * mean_right) / (count_left + count_right)
    return mean


def calc_var(count_left, mean_left, var_left, count_right, mean_right, var_right):
    var = (count_left * var_left + count_right * var_right) / (count_left + count_right) + \
           count_left * count_right * math.pow((mean_left - mean_right) / (count_left + count_right), 2)
    return var


data_size = 0
data_mean = 0
data_var = 0

for line in sys.stdin:
    chunk_size, chunk_mean, chunk_var = map(float, line.strip().split('\t'))

    data_var = calc_var(chunk_size, chunk_mean, chunk_var, data_size, data_mean, data_var)
    data_mean = calc_mean(chunk_size, chunk_mean, data_size, data_mean)
    data_size += chunk_size

print('{}\t{}'.format(data_mean, data_var))
