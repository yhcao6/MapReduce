#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys
import os


# recode in form: item
def read_mapper_output(f, separator):
    for line in f:
        yield line.strip().split(separator)


def main():
    s = float(os.getenv('s'))
    files = os.listdir('output')
    num_baskets = 0
    for file in files:
        if not file.startswith("part"):
            continue
        f = open('output/'+file, 'r')
        tmp = f.readline().strip().split()
        if tmp[0] == chr(0):
            num_baskets = int(tmp[1])
            f.close()
            break
        f.close()
    lowest_support = s * num_baskets

    data = read_mapper_output(sys.stdin, '\t')
    for current_pair, counts in groupby(data, itemgetter(0)):
        total_count = sum(int(count) for current_pair, count in counts)
        if total_count >= lowest_support:
            print("{}\t{}".format(current_pair, total_count))


if __name__ == "__main__":
    main()
