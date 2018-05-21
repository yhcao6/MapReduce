#!/usr/bin/env python

import sys
import numpy as np

K, D = 10, 25


def main():
    current_cluster = None
    current_miu = 0
    current_N = 0

    global_miu = np.zeros((K, D))
    global_N = np.zeros(K)

    for line in sys.stdin:
        k, tmp = line.strip().split("\t")
        k = int(k)
        local_miu, local_N = tmp.split(" ")
        local_miu = np.fromstring(local_miu, sep=',')
        local_N = float(local_N)

        if current_cluster is None or current_cluster == k:
            current_cluster = k
            current_miu += local_miu
            current_N += local_N
        else:
            if current_N != 0:
                miu = current_miu / current_N

            global_miu[current_cluster] = miu
            global_N[current_cluster] = current_N

            current_cluster = k
            current_miu = local_miu
            current_N = local_N

    if current_N != 0:
        miu = current_miu / current_N
    global_miu[current_cluster] = miu
    global_N[current_cluster] = current_N

    pi = global_N / np.sum(global_N)

    for k in range(K):
        miu_k = ','.join([str(v) for v in global_miu[k]])
        pi_k = pi[k]
        print "{}\t{}\t{}".format(k, miu_k, pi_k)


if __name__ == "__main__":
    main()
