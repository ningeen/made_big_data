#!/usr/bin/env python3

import sys
import math
import csv


price = [float(x[9]) for x in csv.reader(sys.stdin) if x[9] != "price"]
chunk_size = len(price)
chunk_mean = sum(price) / chunk_size
chunk_var = sum([math.pow(x - chunk_mean, 2) for x in price]) / chunk_size

print('{}\t{}\t{}'.format(chunk_size, chunk_mean, chunk_var))
