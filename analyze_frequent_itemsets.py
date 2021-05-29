import pickle

import numpy as np
from matplotlib import pyplot as plt


def load_frequent_itemsets():
    with open('C:\\Yash Karia\\Homework\\Fall 2020\\BDA\\Group project\\Output\\frequent_itemsets.pkl', 'rb') as file:
        return pickle.load(file)


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
    frequent_itemsets = load_frequent_itemsets()
    plot_graphs(frequent_itemsets)


if __name__ == '__main__':
    main()
