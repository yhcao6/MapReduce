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
    num_baskets = 0
    s = float(os.getenv('s'))
    sub_file = sys.stdin.read()

    for basket in read_input(sub_file, " "):
        num_baskets += 1
        for it in basket:
            dd[it] += 1

    lowest_support = num_baskets * s
    frequent_items = {k: v for k, v in dd.iteritems() if v >= lowest_support}

    frequent_pairs = defaultdict(zero)
    for basket in read_input(sub_file, " "):
        basket = sorted(basket)
        pairs = combinations(basket, 2)
        for p in pairs:
            if all(k in frequent_items for k in p):
                frequent_pairs[p] += 1

    frequent_pairs = {" ".join(k): v for k, v in frequent_pairs.iteritems()
                      if v >= lowest_support}

    frequent_triples = defaultdict(zero)
    for basket in read_input(sub_file, " "):
        basket = sorted(basket)
        triples = combinations(basket, 3)
        for t in triples:
            p1 = t[0] + " " + t[1]
            p2 = t[1] + " " + t[2]
            p3 = t[0] + " " + t[2]
            if (p1 in frequent_pairs and t[2] in frequent_items) or \
               (p2 in frequent_pairs and t[0] in frequent_items) or \
               (p3 in frequent_pairs and t[1] in frequent_items):
                frequent_triples[t] += 1

    frequent_triples = {k: v for k, v in frequent_triples.iteritems()
                        if v >= lowest_support}

    for k, v in frequent_triples.iteritems():
        print("{}\t{}".format(" ".join(k), v))

    print("{}\t{}".format(chr(0), str(num_baskets)))


if __name__ == '__main__':
    main()
