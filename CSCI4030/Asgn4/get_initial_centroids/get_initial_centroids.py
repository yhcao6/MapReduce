import numpy as np


np.random.seed(0)

X_train = np.genfromtxt('../data/train_img.csv', dtype=float, delimiter=',')
X_label = np.genfromtxt('../data/train_labels.csv', dtype=float, delimiter=',')

N, D = X_train.shape
K = 10

while True:
    # initialize
    initial_centriods = np.zeros((K, D))
    labels = np.zeros(K)
    current_indexes = []
    current_centriods = []

    index = np.random.randint(X_train.shape[0], size=1)[0]  # initial point 1

    current_indexes.append(index)
    current_centriods.append(X_train[index])
    labels[0] = X_label[index]
    print ("{}th initial point index: {}, distance is: {}, \
            label is: {}".format(0, index, 0, X_label[index]))

    for i in range(9):
        max_distance = -np.inf
        max_index = None
        for j in range(X_train.shape[0]):
            distance = 0
            for k in range(len(current_centriods)):
                distance += np.sum((X_train[j] - current_centriods[k])**2)
            if distance > max_distance and j not in current_indexes:
                max_distance = distance
                max_index = j
        print ("{}th initial point index: {}, distance is: {}, \
                label is: {}".format(i+1, max_index, max_distance / len(
                    current_centriods), X_label[max_index]))
        current_indexes.append(max_index)
        current_centriods.append(X_train[max_index])
        labels[i+1] = X_label[max_index]

    for i in range(10):
        initial_centriods[i] = current_centriods[i]

    print np.unique(labels)
    if len(np.unique(labels)) >= 7:
        break

with open('initial_point.txt', 'w') as f:
    for i in range(10):
        centroid = initial_centriods[i]
        centroid = ','.join([str(v) for v in centroid])
        f.write("{}\t{}\t{}\n".format(i, centroid, 0))
