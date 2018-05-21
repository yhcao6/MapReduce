from collections import defaultdict
from itertools import combinations
import operator


def zero():
    return 0


def get_items_counts(file, item_count, num_baskets):
    with open(file, 'r') as f:
        for line in f:
            items = line.strip().split()
            num_baskets += 1
            for item in items:
                item_count[item] += 1
    return num_baskets, item_count


def get_pairs_counts(file, item_count, pairs_counts, lowest_support):
    with open(file, 'r') as f:
        for line in f:
            items = sorted(line.strip().split())
            pairs = combinations(items, 2)
            for p in pairs:
                if all(k in item_count for k in p):
                    pairs_counts[' '.join(p)] += 1
    return pairs_counts


def main():
    # hyper parameters
    files = ['./input/shakespeare-basket1', './input/shakespeare_basket2']
    s = 0.005

    # get item count
    item_count = defaultdict(zero)
    num_baskets = 0
    for file in files:
        num_baskets, item_count = get_items_counts(file, item_count, num_baskets)

    # filter unfrequent items
    lowest_support = s * num_baskets
    for item in item_count.keys():
        if item_count[item] < lowest_support:
            del item_count[item]

    # convert dict to set, since counts are useless
    frequent_items = set(item_count.keys())

    # get frequent pairs
    pairs_counts = defaultdict(zero)
    for file in files:
        pairs_counts = get_pairs_counts(file, frequent_items, pairs_counts, lowest_support)

    # filter unfrequent pairs
    for pair in pairs_counts.keys():
        if pairs_counts[pair] < lowest_support:
            del pairs_counts[pair]

    frequent_pairs = pairs_counts

    # save frequent pairs
    frequent_pairs = sorted(frequent_pairs.items(), key=operator.itemgetter(1), reverse=True)
    with open('res.txt', 'w') as f:
        for pair, count in frequent_pairs[:40]:
            f.write('{}\t{}\n'.format(pair, count))


if __name__ == '__main__':
    main()
