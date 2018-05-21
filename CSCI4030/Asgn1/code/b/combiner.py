#!/usr/bin/python
from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

data = read_mapper_output(sys.stdin)

for key, values in groupby(data, lambda x: x[0]):
    tmp = []
    for k, v in values:
        tmp.append(v)
    print key + "\t" + " ".join(tmp)
