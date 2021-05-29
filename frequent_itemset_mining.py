import sys
from itertools import combinations

import numpy as np
from matplotlib import pyplot as plt
from pymongo import MongoClient


def parse_argv(argv):
    if len(argv) != 3 and len(argv) != 4:
        print("Usage: python3 frequent_itemset_mining.py <value of minimum support>"
              " <maximum allowed size of itemset (0 for no limit)> [MongoDB connection string]")
        sys.exit(1)

    min_support, max_itemset_size = int(argv[1]), int(argv[2])

    if min_support <= 0:
        print("Minimum support has to be a positive integer")
        sys.exit(0)

    if max_itemset_size < 0:
        print("Invalid value of maximum allowed size of itemset")
        sys.exit(0)
    elif max_itemset_size == 0:
        max_itemset_size = float('inf')

    return min_support, max_itemset_size, None if len(argv) == 3 else argv[3]


def get_movies_by_actors(mongodb_connection_string):
    client = MongoClient() if mongodb_connection_string is None else MongoClient(mongodb_connection_string)
    database = client['MapReduce']
    person_roles_collection = database['Person_Roles']

    query = [
        {
            '$match': {
                '$or': [
                    {
                        'category': 'self'
                    }, {
                        'category': 'actor'
                    }, {
                        'category': 'actress'
                    }
                ]
            }
        }, {
            '$limit': 15000
        }, {
            '$project': {
                '_id': '$personId',
                'movieId': '$movieId'
            }
        }, {
            '$group': {
                '_id': '$_id',
                'movies': {
                    '$push': '$movieId'
                }
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }
    ]

    return list(person_roles_collection.aggregate(query, allowDiskUse=True))


def get_first_itemset(movies_by_actors, min_support):
    first_itemset = []

    for actor in movies_by_actors:
        if len(actor['movies']) >= min_support:
            first_itemset.append(actor)

    return first_itemset


def get_frequent_itemsets(first_itemset, min_support, max_itemset_size):
    frequent_itemsets = [[[item['_id']] for item in first_itemset]]
    item_list = first_itemset.copy()
    subset_size = 2

    while len(item_list) != 0 and subset_size <= max_itemset_size:
        subsets = list(combinations(item_list, subset_size))
        new_itemset = []
        item_list = []

        for subset in subsets:
            movies_intersection = set(subset[0]['movies'])

            for index in range(1, subset_size):
                movies_intersection = movies_intersection & set(subset[index]['movies'])

            if len(movies_intersection) >= min_support:
                new_itemset.append([entry['_id'] for entry in subset])

                for entry in subset:
                    if not entry_in_item_list(entry, item_list):
                        item_list.append(entry)

        if len(new_itemset) > 0:
            frequent_itemsets.append(new_itemset)
            subset_size += 1

    return frequent_itemsets


def entry_in_item_list(entry, item_list):
    entry_in_list = False

    for item in item_list:
        if item['_id'] == entry['_id']:
            entry_in_list = True

    return entry_in_list


def plot_graphs(frequent_itemsets):
    num_itemsets = np.arange(len(frequent_itemsets)) + 1
    fig, axes = plt.subplots(1, 2)
    ax = axes.ravel()
    ax[0] = plot_num_frequent_itemsets(frequent_itemsets, num_itemsets, ax[0])
    ax[1] = plot_num_unique_actors(frequent_itemsets, num_itemsets, ax[1])
    plt.show()


def plot_num_frequent_itemsets(frequent_itemsets, num_itemsets, ax):
    num_frequent_itemsets = [len(level) for level in frequent_itemsets]

    ax.bar(num_itemsets, num_frequent_itemsets)
    ax.set_title('Number of frequent itemsets per level')
    ax.set_xlabel('Level')
    ax.set_ylabel('Number of frequent itemsets')

    return ax


def plot_num_unique_actors(frequent_itemsets, num_itemsets, ax):
    unique_actors = []

    for level in frequent_itemsets:
        level_list = []

        for frequent_itemset in level:
            level_list.extend(frequent_itemset)

        unique_actors.append(level_list)

    unique_actors = [list(set(level)) for level in unique_actors]
    num_unique_actors = [len(level) for level in unique_actors]

    ax.bar(num_itemsets, num_unique_actors)
    ax.set_title('Number of unique actors per level')
    ax.set_xlabel('Level')
    ax.set_ylabel('Number of unique actors')

    return ax


def main():
    min_support, max_itemset_size, mongodb_connection_string = parse_argv(sys.argv)
    movies_by_actors = get_movies_by_actors(mongodb_connection_string)
    first_itemset = get_first_itemset(movies_by_actors, min_support)

    if len(first_itemset) == 0:
        print("No frequent itemsets can be generated for the given minimum support")
        sys.exit(0)

    frequent_itemsets = get_frequent_itemsets(first_itemset, min_support, max_itemset_size)
    plot_graphs(frequent_itemsets)


if __name__ == '__main__':
    main()
