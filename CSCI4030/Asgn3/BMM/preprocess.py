import numpy as np


X_train = np.genfromtxt('./ori_train_img.csv', dtype=int, delimiter=',')
X_test = np.genfromtxt('./ori_test_img.csv', dtype=int, delimiter=',')

# binarilization
X_train[X_train<128] = 0
X_train[X_train!=0] = 1
X_test[X_test<128] = 0
X_test[X_test!=0] = 1

np.savetxt('train_image.csv', X_train, fmt='%i', delimiter=',')
np.savetxt('test_image.csv', X_test, fmt='%i', delimiter=',')
