import os
import struct
import numpy as np
from sklearn.decomposition import PCA


def load_mnist(path, kind='train'):
    if kind == 'train':
        labels_path = './ori_data/train-labels-idx1-ubyte'
        images_path = './ori_data/train-images-idx3-ubyte'
    elif kind == 'test':
        images_path = './ori_data/t10k-images-idx3-ubyte'
        labels_path = './ori_data/t10k-labels-idx1-ubyte'

    with open(labels_path, 'rb') as lbpath:
        magic, n = struct.unpack('>II', lbpath.read(8))
        labels = np.fromfile(lbpath, dtype=np.uint8)

    with open(images_path, 'rb') as imgpath:
        magic, num, rows, cols = struct.unpack('>IIII', imgpath.read(16))
        images = np.fromfile(imgpath, dtype=np.uint8).reshape(len(labels), 784)

    return images, labels


pca = PCA(n_components=25)
X_train, y_train = load_mnist("./")
X_train = pca.fit_transform(X_train)
X_test, y_test = load_mnist("./", kind='test')
X_test = pca.transform(X_test)

np.savetxt('data/train_img.csv', X_train, fmt='%f', delimiter=',')
np.savetxt('data/train_labels.csv', y_train, fmt='%f', delimiter=',')
np.savetxt('data/test_img.csv', X_test, fmt='%f', delimiter=',')
np.savetxt('data/test_labels.csv', y_test, fmt='%f', delimiter=',')
