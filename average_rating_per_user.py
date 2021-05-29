# This program plots a scatter plot showing the average rating of each user

import matplotlib.pyplot as matplot
from pymongo import MongoClient


def main():
    query = [
        {"$group": {"_id": "$userId", "avgRating": {"$avg": "$rating"}}}
    ]

    result = list(movies.aggregate(query))
    time_series_plot(result)


def time_series_plot(result):
    x = []
    y = []
    new_list = sorted(result, key=lambda k: k['_id'])
    for r in new_list:
        x.append(r['_id'])
        y.append(r['avgRating'])

    matplot.title('Average rating given by each user')
    matplot.scatter(x, y, s=1)
    matplot.xlabel("User ID")
    matplot.ylabel("Average Rating")
    matplot.show()
    print("Finished plotting time series graph")


if __name__ == '__main__':
    client = MongoClient()
    db = client['MapReduce']
    movies = db['Ratings']
    main()
