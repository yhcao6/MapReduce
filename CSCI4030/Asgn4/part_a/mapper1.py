#!/usr/bin/env python
import sys
import numpy as np


def zero():
    return 0 


def main():

    # read cluster points
    centroids = np.zeros((10, 25))
    with open('./old_centroid_points.txt', 'r') as f:
        i = 0
        for line in f:
            index, centroid, counts = line.strip().split("\t")
            index = int(float(index))
            centroid = np.fromstring(centroid, sep=',')
            centroids[index] = centroid
            i += 1

    local_centroid = np.zeros((10, 25))
    local_centroid_counts = np.zeros((10, 1))

    for line in sys.stdin:
        p = np.fromstring(line, sep=',')
        distances = np.sum((centroids - p) ** 2, axis=1)
        min_index = np.argmin(distances)

        local_centroid[min_index] += p
        local_centroid_counts[min_index] += 1

    for i in range(len(local_centroid_counts)):
        centroid = ','.join([str(v) for v in local_centroid[i]])
        print str(i) + '\t' + centroid +  '\t' + str(local_centroid_counts[i, 0])


if __name__ == '__main__':
    main()
