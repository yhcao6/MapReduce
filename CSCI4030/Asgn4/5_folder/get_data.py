import numpy as np
import os


np.random.seed(0)

X_train = np.genfromtxt('../data/mixture_img.csv', delimiter=',')
X_label = np.genfromtxt('../data/mixture_labels.csv', delimiter=',')

n_folder = 5
interval = X_train.shape[0] / n_folder

# shuffle
indices = np.random.permutation(X_train.shape[0])
X_train = X_train[indices]
X_label = X_label[indices]

if os.path.isdir('5_folder_input'):
    os.system("rm -rf 5_folder_input")
os.system("mkdir 5_folder_input")

# split into n folders
for i in range(n_folder):
    # test folder
    test_folder_num = i
    test_indices = range(test_folder_num * interval,
                         (test_folder_num + 1) * interval)
    test_image = X_train[test_indices]
    test_label = X_label[test_indices]

    # train folder
    train_image = np.delete(X_train, test_indices, axis=0)
    train_label = np.delete(X_label, test_indices)

    if os.path.isdir('5_folder_input/iter_{}'.format(i)):
        os.system("rm -rf 5_folder_input/iter_{}".format(i))
    os.mkdir('5_folder_input/iter_{}'.format(i))

    np.savetxt('5_folder_input/iter_{}/train_image.csv'.format(i),
               train_image, fmt='%f', delimiter=',')
    np.savetxt('5_folder_input/iter_{}/train_label.csv'.format(i),
               train_label, fmt='%f', delimiter=',')
    np.savetxt('5_folder_input/iter_{}/test_image.csv'.format(i),
               test_image, fmt='%f', delimiter=',')
    np.savetxt('5_folder_input/iter_{}/test_label.csv'.format(i),
               test_label, fmt='%f', delimiter=',')
