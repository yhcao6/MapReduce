import numpy as np


X_train = np.genfromtxt("./train_image.csv", delimiter=",").astype(np.float128)
X_label = np.genfromtxt("./train_labels.csv", delimiter=",").astype(np.float128)
X_test = np.genfromtxt("./test_image.csv", delimiter=",").astype(np.float128)
y_test = np.genfromtxt("./test_labels.csv", delimiter=",").astype(np.float128)

N = X_train.shape[0]
D = X_train.shape[1]
K = 10


Q = np.genfromtxt("./centroid_points.txt", delimiter=",").astype(np.float128)
Pi = np.ones(K).astype(np.float128) / K
last_Q = None
last_Pi = None
error = np.inf

iteration = 0

while iteration < 20 and error > 0:
    # E step
    z_n_k = np.zeros((N, K)).astype(np.float128)
    for n in range(N):
        x = X_train[n]
        tmp = Pi * np.prod(Q ** x * (1 - Q) ** (1 - x), axis=1)
        z_n_k[n] = tmp / np.sum(tmp)

    # M step
    for k in range(K):
        Q[k] = np.sum(z_n_k[:, k].reshape(-1, 1) * X_train, axis=0) / np.sum(z_n_k[:, k])
    Pi = np.sum(z_n_k, axis=0) / N

    iteration += 1

    if last_Pi is None:
        error = np.inf
    else:
        error_Pi = np.sum((Pi - last_Pi) ** 2)
        error_Q = np.sum((Q - last_Q) ** 2)
        error = error_Pi + error_Q

    last_Pi = Pi
    last_Q = Q

    print("iteration: {}, error: {}".format(iteration, error))

predictions = np.zeros(y_test.shape[0])
counts = np.zeros((K, K))

for n in range(X_test.shape[0]):
    x = X_test[n]
    tmp = Pi * np.prod(Q ** x * (1 - Q) ** (1 - x), axis=1)
    cluster = np.argmax(tmp)
    label = int(y_test[n])
    counts[cluster, label] += 1
    predictions[n] = cluster

cluster = {}
for k in range(K):
    cluster[k] = np.argmax(counts[k])
    print "cluster:", k
    print counts[k]

for n in range(y_test.shape[0]):
    predictions[n] = cluster[predictions[n]]

for k in cluster.keys():
    num = np.sum(counts[k])
    if num == 0:
        a = 0
    else:
        a = np.max(counts[k]) / num
    print "cluster: {}, num: {}, threshold is: {}, true label is: {}, correctly_clustered_images: {}, accuracy: {}".format( k, num, num, cluster[k], np.max(counts[k]), a)

print "accuracy: {}".format(np.sum(predictions == y_test) / float(y_test.shape[0]))

