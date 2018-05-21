import numpy as np


K, D = 10, 5

mu = np.zeros((K, D))
cov = np.zeros((K, D*D))
pi = np.zeros((K, ))

with open('output2/part-00000', 'r') as f:
    for line in f:
        k, mu_k, cov_k, pi_k = line.strip().split('\t')
        k = int(k)
        mu[k] = np.fromstring(mu_k, sep=',')
        cov[k] = np.fromstring(cov_k, sep=',')
        pi[k] = float(pi_k)

np.savetxt('ori_miu.txt', mu, fmt='%f', delimiter=',')
np.savetxt('ori_cov.txt', cov, fmt='%f', delimiter=',')
np.savetxt('ori_pi.txt', pi, fmt='%f', delimiter=',')
