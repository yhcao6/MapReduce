#!/usr/bin/env python

import sys
import numpy as np

K, D = 10, 25


def read_parameters():
    miu = np.zeros((K, D))
    pi = np.zeros((K,))
    with open('output/part-00000', 'r') as f:
        for line in f:
            k, miu_k, pi_k = line.strip().split('\t')
            k = int(k)
            miu_k = np.fromstring(miu_k, sep=',')
            miu[k] = miu_k
            pi[k] = pi_k
    return miu, pi


def main():
    current_cluster = None
    current_cov = 0
    current_N = 0

    global_miu, pi = read_parameters()
    global_N = np.zeros(K)
    global_cov = np.zeros((K, D*D))

    for line in sys.stdin:
        k, tmp = line.strip().split("\t")
        local_cov, local_N = tmp.split(" ")

        k = int(k)
        local_cov = np.fromstring(local_cov, sep=',')
        local_N = float(local_N)

        if current_cluster is None or current_cluster == k:
            current_cluster = k
            current_cov += local_cov
            current_N += local_N
        else:
            if current_N != 0:
                current_cov = current_cov / current_N

            global_cov[current_cluster] = current_cov
            global_N[current_cluster] = current_N

            current_cluster = k
            current_cov = local_cov
            current_N = local_N

    if current_N != 0:
        current_cov = current_cov / current_N

    global_cov[current_cluster] = current_cov
    global_N[current_cluster] = current_N

    for k in range(K):
        miu = ','.join([str(v) for v in global_miu[k]])
        cov = ','.join([str(v) for v in global_cov[k]])
        pi = global_N[k] / np.sum(global_N)
        print "{}\t{}\t{}\t{}".format(k, miu, cov, pi)


if __name__ == "__main__":
    main()
