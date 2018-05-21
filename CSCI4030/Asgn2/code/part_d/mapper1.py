#!/usr/bin/python
import sys
from collections import defaultdict
from itertools import combinations
import os


def zero():
    return 0


def read_input(data, separator=" "):
    for line in data.splitlines():
        yield line.strip().split(separator)


def main():
    dd = defaultdict(zero)
    pcy_pair_count = defaultdict(zero)
    num_baskets = 0
    s = float(os.getenv('s'))
    sub_file = sys.stdin.read()

    for basket in read_input(sub_file, " "):
        num_baskets += 1
        for it in basket:
            dd[it] += 1
        basket = sorted(basket)
        pairs = combinations(basket, 2)
        for p in pairs:
            pcy_pair_count[hash(p[0] + p[1]) % 100000] += 1

    lowest_support = num_baskets * s
    frequent_items = {k: v for k, v in dd.iteritems() if v >= lowest_support}

    frequent_pairs = defaultdict(zero)
    for basket in read_input(sub_file, " "):
	basket = sorted(basket)
        pairs = combinations(basket, 2)
        for p in pairs:
            if all(k in frequent_items for k in p) and pcy_pair_count[hash(p[0] + p[1]) % 100000] >= lowest_support:
                frequent_pairs[p] += 1

    frequent_pairs = {k: v for k, v in frequent_pairs.iteritems() if v >= lowest_support}
    for k, v in frequent_pairs.iteritems():
        print("{}\t{}".format(k[0] + " " + k[1], v))

    print("{}\t{}".format(chr(0), str(num_baskets)))


if __name__ == '__main__':
    main()
