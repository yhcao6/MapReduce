#!/usr/bin/env python
import sys
import numpy as np


K, D = 10, 25


def pdf(x, mu, cov):
    D = x.shape[0]
    part1 = 1 / ((2 * np.pi) ** (D / 2) * np.linalg.det(cov) ** 0.5)
    tmp = (x - mu).reshape(-1, 1)
    part2 = -(np.matmul(np.matmul(tmp.T, np.linalg.inv(cov)), tmp).item()) / 2.0
    return float(part1 * np.exp(part2))


def read_parameters():
    miu = np.zeros((K, D))
    pi = np.zeros((K,))
    cov = np.zeros((K, D*D))
    with open('ori_params.txt', 'r') as f:
        for line in f:
            k, miu_k, cov_k, pi_k = line.strip().split('\t')
            miu_k = np.fromstring(miu_k, sep=',')
            cov_k = np.fromstring(cov_k, sep=',')
            pi_k = float(pi_k)

            k = int(k)
            miu[k] = miu_k
            cov[k] = cov_k
            pi[k] = pi_k
    return miu, cov, pi


def main():

    miu, cov, pi = read_parameters()

    local_miu = np.zeros((K, D)).astype(np.float64)
    local_N = np.zeros((K,)).astype(np.float64)

    for line in sys.stdin:
        x = np.fromstring(line, sep=',')
        numerators = np.zeros((K,))

        # calculate gamma
        for k in range(K):
            numerators[k] = pi[k] * pdf(x, miu[k], cov[k].reshape(D, D))
        gamma = numerators / np.sum(numerators)

        # calclulate partial sum
        for k in range(K):
            local_miu[k] += gamma[k] * x
            local_N[k] += gamma[k]

        # emit in form: k local_miu[k] local_N[k]
        for k in range(K):
            m = ','.join([str(v) for v in local_miu[k]])
            print '{}\t{} {}'.format(k, m, local_N[k])


if __name__ == '__main__':
    main()
