import numpy as np


centroids = np.zeros((10, 25))
cluster_count = np.zeros(10)


with open('./new_centroid_points.txt', 'r') as f:
    for line in f:
        line = line.strip().split('\t')
        cluster_id, centroid, count = line
        cluster_id = int(float(cluster_id))

        centroid = np.fromstring(centroid, sep=',')
        centroids[cluster_id] = centroid

        count = int(float(count))
        cluster_count[cluster_id] = count


with open('./res_a', 'w') as out:
    for i in range(10):
        centroid = np.round(centroids[i], 3)
        centroid = ','.join(str(v) for v in centroid)
        out.write("Centroid {}: [{}], {}\n".format(i, centroid, cluster_count[i]))
