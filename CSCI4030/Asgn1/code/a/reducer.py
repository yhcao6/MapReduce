#!/usr/bin/python
from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

data = read_mapper_output(sys.stdin)

previous_key = None
k1, v1, l1, t1 = None, None, 0, None
k2, v2, l2, t2 = None, None, 0, None
k3, v3, l3, t2 = None, None, 0, None

for key, values in groupby(data, lambda x: x[0]):
    current_k1, current_k2 = key.split(" ")
    if previous_key != current_k1:
        if previous_key:
            t = []
            for k in k1, k2, k3:
                if k:
                    t.append(k)
            t.sort(key=int)
            print previous_key + ":" + ",".join(t)
        previous_key = current_k1
        k1, v1, l1, t1 = None, None, 0, None
        k2, v2, l2, t2 = None, None, 0, None
        k3, v3, l3, t3 = None, None, 0, None

    tmp = []
    for k, v in values:
        for vv in v.split(" "):
            tmp.append(int(vv))
    l = len(tmp)

    if l > l1:
        k3, v3, l3, t3 = k2, v2, l2, t2
        k2, v2, l2, t2 = k1, v1, l1, t1
        tmp = sorted(tmp)
        k1, v1, l1, t1 = current_k2, ",".join(str(x) for x in tmp), l, tmp
    elif l > l2:
        k3, v3, l3, t3 = k2, v2, l2, t2
        tmp = sorted(tmp)
        k2, v2, l2, t2 = current_k2, ",".join(str(x) for x in tmp), l, tmp
    elif l > l3:
        tmp = sorted(tmp)
        k3, v3, l3, t3 = current_k2, ",".join(str(x) for x in tmp), l, tmp

if previous_key:
    t = []
    for k in k1, k2, k3:
        if k:
            t.append(k)
    t.sort(key=int)
    print previous_key + ":" + ",".join(t)


