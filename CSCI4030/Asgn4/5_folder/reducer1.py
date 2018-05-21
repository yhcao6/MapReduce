#!/usr/bin/env python

import sys
sys.path.append('/home/ubuntu/miniconda2/lib/python2.7/site-packages/')
import numpy as np


def main():
    current_centroid_index = None
    current_counts = 0.

    for line in sys.stdin:
        centroid_index, local_centroid, local_counts = line.strip().split("\t")

        centroid_index = int(centroid_index)
        local_counts = float(local_counts)
        local_centroid = np.fromstring(local_centroid, sep=',')

        if current_centroid_index is None:
            current_centroid_index = centroid_index
            current_counts = local_counts
            current_centroid = local_centroid
        elif centroid_index != current_centroid_index:
            if current_counts != 0:
                current_centroid = current_centroid / current_counts
            p = ','.join([str(v) for v in current_centroid])
            print "{}\t{}\t{}".format(current_centroid_index, p, current_counts)

            current_counts = local_counts
            current_centroid_index = centroid_index
            current_centroid = local_centroid
        else:
            current_centroid += local_centroid
            current_counts += local_counts

    if current_counts != 0:
        current_centroid = current_centroid / current_counts
    p = ','.join([str(v) for v in current_centroid])
    print "{}\t{}\t{}".format(current_centroid_index, p, current_counts)

if __name__ == "__main__":
    main()
