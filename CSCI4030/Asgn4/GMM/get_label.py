import numpy as np
from scipy.stats import multivariate_normal


K, D = 10, 25


miu = np.zeros((K, D))
pi = np.zeros((K,))
cov = np.zeros((K, D*D))


with open('res', 'r') as f:
    for line in f:
        k, miu_k, cov_k, pi_k = line.strip().split('\t')
        miu_k = np.fromstring(miu_k, sep=',')
        cov_k = np.fromstring(cov_k, sep=',')
        pi_k = float(pi_k)

        k = int(k)
        miu[k] = miu_k
        cov[k] = cov_k
        pi[k] = pi_k

models = []
for k in range(K):
    models.append(multivariate_normal(
        mean=miu[k], cov=cov[k].reshape(D, D)))

X_test = np.genfromtxt("./data/test_img.csv",
                       delimiter=",").astype(np.float64)
y_test = np.genfromtxt("./data/test_labels.csv",
                       delimiter=",").astype(np.float64)


predictions = np.zeros(y_test.shape[0])
counts = np.zeros((K, K))

for n in range(X_test.shape[0]):
    x = X_test[n]
    tmp = np.zeros((10, ))  # likelihood
    for k in range(K):
        tmp[k] = pi[k] * models[k].pdf(x)
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
    print "cluster: {}, num: {}, threshold is: {}, true label is: {}, correctly_clustered_images: {}, accuracy: {}".format(k, num, num, cluster[k], np.max(counts[k]), a)

print "accuracy: {}".format(np.sum(predictions == y_test) / float(y_test.shape[0]))

