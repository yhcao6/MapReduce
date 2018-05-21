import numpy as np


K, D = 10, 25

new_centroids = np.zeros((K, D))
old_centroids = np.zeros((K, D))

with open('./new_centroid_points.txt', 'r') as f:
    for line in f:
        index, centroid, count = line.strip().split("\t")
        index = int(float(index))
        centroid = np.fromstring(centroid, sep=',')
        count = int(float(count))
        new_centroids[index] = centroid

with open('./old_centroid_points.txt', 'r') as f:
    for line in f:
        index, centroid, count = line.strip().split("\t")
        index = int(float(index))
        centroid = np.fromstring(centroid, sep=',')
        count = int(float(count))
        old_centroids[index] = centroid

print int(np.sum((new_centroids - old_centroids) ** 2))
