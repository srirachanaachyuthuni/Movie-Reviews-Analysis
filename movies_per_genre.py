# Plots a bar graph showing the total number of movies in each genre

import matplotlib.pyplot as matplot
from pymongo import MongoClient


def main():
    query = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}}
    ]
    result = list(movies.aggregate(query))
    bar_graph_plot(result)


def bar_graph_plot(result):
    genres = []
    total = []
    for r in result:
        if r['_id'] != "\\N" and r['_id'] != "Talk-Show":
            genres.append(r['_id'])
            total.append(r['count'])

    matplot.title('Total number of movies per genre')
    matplot.xticks(rotation=45)
    matplot.bar(genres, total)
    matplot.xlabel("Genre")
    matplot.ylabel("Total number of movies")
    matplot.show()
    print("Finished plotting bar graph")


if __name__ == '__main__':
    client = MongoClient()
    db = client['MapReduce']
    movies = db['Movie']
    main()
