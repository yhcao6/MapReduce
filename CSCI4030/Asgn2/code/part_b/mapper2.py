#!/usr/bin/python
import sys
from itertools import combinations
import os


def read_input(f, separator=" "):
    for line in f:
        yield line.strip().split(separator)


frequent_pairs = []

files = os.listdir('output')
for file in files:
    if not file.startswith('part'):
        continue
    f = open('output/'+file, 'r')
    line = read_input(f, '\t')

    for pair, count in line:
        frequent_pairs.append(pair)

frequent_pairs = set(frequent_pairs)
frequent_pairs.discard(chr(0))

line = read_input(sys.stdin, ' ')

for basket in read_input(sys.stdin, ' '):
    basket = sorted(basket)
    pairs = combinations(basket, 2)
    for p in pairs:
        p = ' '.join(p)
        if p in frequent_pairs:
            print("{}\t{}".format(p, 1))
