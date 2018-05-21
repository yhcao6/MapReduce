#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys


# recode in form: item
def read_mapper_output(f, separator):
    for line in f:
        yield line.strip().split(separator)


def main():
    data = read_mapper_output(sys.stdin, '\t')
    for current_pair, counts in groupby(data, itemgetter(0)):
        total_count = sum(int(count) for current_pair, count in counts)
        print("{}\t{}".format(current_pair, total_count))


if __name__ == "__main__":
    main()
