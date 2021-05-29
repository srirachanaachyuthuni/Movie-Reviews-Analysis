import sys
from math import sqrt, pow
from random import random

from matplotlib import pyplot as plt
from matplotlib import colors as mcolors
from pymongo import MongoClient


def parse_argv(argv):
    if not 2 <= len(argv) <= 4:
        print_usage()
        sys.exit(1)

    mode = argv[1]

    if mode == '--k':
        k = int(argv[2])

        if k < 1:
            print("Invalid k")
            sys.exit(1)

        return k, None if len(argv) == 3 else argv[3]
    elif mode == '--sse':
        return -1, None if len(argv) == 2 else argv[2]
    else:
        print_usage()
        sys.exit(1)


def print_usage():
    print("Usages:\n1. To run k-means for single k: python3 clustering.py --k <value of k> [MongoDB connection string]")
    print("2. To see the SSE curve for k = 1 to k = 10: python3 clustering.py --sse [MongoDB connection string]")


def get_result(mongodb_connection_string):
    client = MongoClient() if mongodb_connection_string is None else MongoClient(mongodb_connection_string)
    db = client['MapReduce']
    ratings = db['Ratings']

    query = [
        {"$group": {"_id": {"movieId": "$movieId"}, "avgRating": {"$avg": "$rating"}}}
    ]

    return list(ratings.aggregate(query))


def k_means(ratings, k):
    ratings = create_normalized_field(ratings)
    centroids = create_centroids(k)
    return run_k_means(ratings, centroids, k)


def create_normalized_field(ratings):
    minimum = min([rating_document['avgRating'] for rating_document in ratings])
    maximum = max([rating_document['avgRating'] for rating_document in ratings])

    for rating_document in ratings:
        normalized_avg_rating = ((rating_document['avgRating'] - minimum) / (maximum - minimum))
        rating_document['normalizedAvgRating'] = normalized_avg_rating

    return ratings


def create_centroids(k):
    return [random() for _ in range(k)]


def find_centroid(rating_document, centroids):
    normalized_rating = rating_document['normalizedAvgRating']
    min_distance = sqrt(pow(normalized_rating - centroids[0], 2))
    closest_centroid = centroids[0]

    for centroid in centroids:
        distance = sqrt(pow(normalized_rating - centroid, 2))
        if distance < min_distance:
            min_distance = distance
            closest_centroid = centroid

    return closest_centroid


def find_average_centroid(cluster):
    total, count = 0, 0

    for point in cluster:
        total += point['normalizedAvgRating']
        count += 1

    return total / count


def run_k_means(ratings, centroids, k):
    clusters = {}
    new_centroids = []
    run_again = False

    for rating_document in ratings:
        centroid = find_centroid(rating_document, centroids)
        if centroid not in clusters:
            clusters[centroid] = [rating_document]
        else:
            clusters[centroid].append(rating_document)

    for centroid in clusters:
        if len(clusters[centroid]) == 0:
            new_centroids.append(random())
        else:
            new_centroids.append(find_average_centroid(clusters[centroid]))

    if len(centroids) != len(new_centroids) or len(centroids) != k or len(new_centroids) != k:
        run_again = True

    for centroid in centroids:
        if centroid not in new_centroids:
            run_again = True
            break

    if run_again:
        return run_k_means(ratings, new_centroids, k)
    else:
        return clusters


def plot_clusters(clusters, k):
    ratings = []
    colors = []
    color_list = [mcolors.TABLEAU_COLORS[color] for color in mcolors.TABLEAU_COLORS]
    cluster_counter = 1

    for centroid in clusters:
        for point in clusters[centroid]:
            ratings.append(point['normalizedAvgRating'])
            colors.append(color_list[(cluster_counter - 1) % 10])

        cluster_counter += 1

    plt.scatter(ratings, ratings, color=colors)
    plt.ylabel('Average Ratings')
    plt.xlabel('Average Ratings')
    plt.title('k = {} clusters based on average ratings'.format(k))
    plt.show()


def plot_sse_curve(ratings, max_k):
    sse_list = get_sse_list(ratings, max_k)
    k_list = [i for i in range(1, max_k + 1)]

    plt.title("Plot of number of clusters (k) against its Sum of Squared Errors (SSE)")
    plt.plot(k_list, sse_list)
    plt.xticks(k_list)
    plt.xlabel('k')
    plt.ylabel('SSE')
    plt.show()


def get_sse_list(ratings, max_k):
    sse_list = []

    for k in range(1, max_k + 1):
        clusters = k_means(ratings, k)
        sse_list.append(compute_sse(clusters))

    return sse_list


def compute_sse(clusters):
    sse = 0

    for centroid in clusters:
        for point in clusters[centroid]:
            sse += pow(point['normalizedAvgRating'] - centroid, 2)

    return sse


def main():
    k, mongodb_connection_string = parse_argv(sys.argv)
    ratings = get_result(mongodb_connection_string)

    if k == -1:
        plot_sse_curve(ratings, 10)
    else:
        clusters = k_means(ratings, k)
        plot_clusters(clusters, k)


if __name__ == '__main__':
    main()
