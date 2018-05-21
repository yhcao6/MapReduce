import numpy

numpy.random.seed(0)


def matrix_factorization(R, P, Q, K, alpha=0.0002, beta=0.02):
    Q = Q.T
    e = 0
    last_e = 0
    step = 0
    while True:
        for i in xrange(len(R)):
            for j in xrange(len(R[i])):
                if R[i][j] > 0:
                    eij = R[i][j] - numpy.dot(P[i, :], Q[:, j])
                    for k in xrange(K):
                        tmp = P[i][k]
                        t1 = P[i][k] + alpha * (
                                2 * eij * Q[k][j] - beta * P[i][k])
                        t2 = Q[k][j] + alpha * (
                                2 * eij * tmp - beta * Q[k][j])
                        P[i][k] = t1
                        Q[k][j] = t2
        e = 0
        for i in xrange(len(R)):
            for j in xrange(len(R[i])):
                if R[i][j] > 0:
                    e = e + pow(R[i][j] - numpy.dot(P[i, :], Q[:, j]), 2)
                    for k in xrange(K):
                        e = e + (beta/2) * (pow(P[i][k], 2) + pow(Q[k][j], 2))
        print 'step: {}, error: {}, diff: {}'.format(step, e, numpy.abs(e - last_e))
        if numpy.abs(e - last_e) < 1e-8:
            break
        else:
            last_e = e
            step += 1
        # if e < 0.001:
        #     break
    return P, Q.T


if __name__ == "__main__":
    R = numpy.array([[1, 1, 6, 4, 4, 0],
                     [0, 3, 0, 4, 5, 4],
                     [6, 0, 0, 2, 4, 4],
                     [2, 1, 4, 5, 0, 5],
                     [4, 4, 2, 0, 3, 1]]).astype(float)

    R = numpy.array(R)

    N = len(R)
    M = len(R[0])
    K = 2

    P = numpy.random.rand(N, K)
    Q = numpy.random.rand(M, K)

    nP, nQ = matrix_factorization(R, P, Q, K)
    nR = numpy.dot(nP, nQ.T)
    print nR
