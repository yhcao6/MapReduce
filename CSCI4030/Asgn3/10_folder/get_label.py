import numpy as np


best_accuracy = []

for turn in range(10):
    centroids = np.zeros((10, 784))
    centroid_file = "iter_{}_centroid_points.txt".format(turn)
    test_img_file = "10_folder_input/iter_{}/test_image.csv".format(turn)
    test_label_file = "10_folder_input/iter_{}/test_label.csv".format(turn)
    turn_accuracies = []
    
    i = 0
    with open(centroid_file, 'r') as f:
        for line in f:
            # begin = line.find('[')
            # end = line.find(']')
            # centroid = line[begin+1: end]
            centroid = np.fromstring(line.strip().split("\t")[1], sep=',')
            centroids[i] = centroid
    
            i += 1
    
    
    predictions = [[] for i in range(10)]
    
    with open(test_img_file, 'r') as f:
        i = 0
        for line in f:
            p = np.fromstring(line, sep=',')
            distances = np.sum((centroids - p) ** 2, axis=1)
            min_index = np.argmin(distances)
            min_distances = distances[min_index]
            predictions[min_index].append((i, min_distances))
            i += 1
    
    
    nums = np.zeros((10, 1))
    for i in range(10):
        predictions[i] = sorted(predictions[i], key=lambda x: x[1])
        nums[i] = len(predictions[i])
    
    
    labels = np.genfromtxt(test_label_file, delimiter=',')
    
    for s in [0.05, 0.1, 0.5, 1]:
        print("turn is: {}, threshold is: {}".format(turn, s))
        
        correctly_clustered_images = np.zeros(10)
        accuracies = np.zeros(10)
        
        res = np.zeros(labels.shape[0])
        
        for i in range(10):
            tmp = np.zeros(10)
            threshold = s * nums[i]
            above = False
        
            for p in predictions[i]:
                index = p[0]
                label = int(labels[index])
                tmp[label] += 1
                if tmp[label] >= threshold:
                    above = True
        
                    # calculate accuracy
                    correct_num = 0
                    for pair in predictions[i]:
                        j = pair[0]
                        res[j] = label
                        if res[j] == labels[j]:
                            correct_num += 1
                    accuracy = correct_num / float(len(predictions[i]))
                    print "cluster: {}, num is: {}, threshold is: {}, true label is: {}, correctly_clustered_images: {}, accuracy is: {}".format( i, nums[i], threshold, label, nums[i]*accuracy, np.round(accuracy, 3))
                    # print tmp
                    correctly_clustered_images[i] = nums[i]*accuracy
                    accuracies[i] = accuracy
                    break
                
            if not above:
                label = np.argmax(tmp)
        
                # calculate accuracy
                correct_num = 0
        
                for pair in predictions[i]:
                    j = pair[0]
                    res[j] = label
                    if res[j] == labels[j]:
                        correct_num += 1
        
                accuracy = correct_num / float(len(predictions[i]))
        
                print "cluster: {}, num is: {}, threshold is: {}, true label is: {}, correctly_clustered_images: {}, accuracy is: {}".format( i, nums[i], threshold, label, nums[i]*accuracy, np.round(accuracy, 3))
                # print tmp
        
                correctly_clustered_images[i] = nums[i]*accuracy
                accuracies[i] = accuracy
        
        print "accuracy is: {}".format(np.sum(res==labels) / float(len(labels)))
        
        print "correctly_clustered_images: {}".format(np.sum(correctly_clustered_images))
        print "\n"
        turn_accuracies.append(np.round(np.sum(res==labels) / float(len(labels)), 4))
    best_accuracy.append(max(turn_accuracies))
print best_accuracy
print "average is: {}".format(np.mean(best_accuracy))
