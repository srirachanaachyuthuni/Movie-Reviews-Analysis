# plotting a time series graph showing the total number of movies released per year

import matplotlib.pyplot as matplot
from pymongo import MongoClient


def main():
    query = [
        {"$group": {"_id": "$startYear", "count": {"$sum": 1}}}
    ]
    result = list(movies.aggregate(query))
    time_series_plot(result)


def time_series_plot(result):
    x = []
    y = []
    for r in result:
        if not r['_id']:
            result.remove(r)
    new_list = sorted(result, key=lambda k: k['_id'])

    for r in new_list:
        x.append(r['_id'])
        y.append(r['count'])

    matplot.title('Number of movies per year')
    matplot.plot(x, y)
    matplot.xlabel("Start Year of the movie")
    matplot.ylabel("Total number of movies")
    matplot.show()


if __name__ == '__main__':
    client = MongoClient()
    db = client['MapReduce']
    movies = db['Movie']
    main()
