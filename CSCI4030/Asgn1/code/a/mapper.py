#!/usr/bin/python

import sys

for line in sys.stdin:

    line = line.strip()

    common, words = line.split(":")
    words = words.split(" ")

    for i in range(len(words)):
        for j in range(i+1, len(words)):
            print words[i], words[j] + "\t" + common
            print words[j], words[i] + "\t" + common
