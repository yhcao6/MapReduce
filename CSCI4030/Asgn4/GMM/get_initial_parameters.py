import numpy as np


K, D = 10, 25

X = np.genfromtxt('./data/train_img.csv', delimiter=',')
N = X.shape[0]
interval = N / K

tmp = np.mean(X, axis=0)
mu = np.zeros((K, D))
cov = np.zeros((K, D*D))

for k in range(K):
    x = X[k * interval: (k+1) * interval]
    mu[k] = np.mean(x, axis=0).reshape(-1)
    cov[k] = np.cov(x.T).reshape(-1)

pi = np.ones(10,)
pi = pi / np.sum(pi)

with open('ori_params.txt', 'w') as f:
    for k in range(K):
        m = ','.join([str(v) for v in mu[k]])
        c = ','.join([str(v) for v in cov[k]])
        f.write('{}\t{}\t{}\t{}\n'.format(k, m, c, pi[k]))
