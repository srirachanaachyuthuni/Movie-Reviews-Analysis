For merge_datasets.py:
Usage: python3 merge_datasets.py <directory containing the IMDb dataset> <directory containing the MovieLens dataset> [MongoDB connection string]

Provide the following in the command-line arguments:
1. The directory containing the IMDb dataset. This directory should contain the following files:
	a. name.basics.tsv.gz
	b. title.basics.tsv.gz
	c. title.principals.tsv.gz
2. The directory containing the MovieLens dataset. This directory should contain the following files:
	a. links.csv
	b. ratings.csv
3. The optional parameter of the MongoDB connection string. If provided, the program will insert the data into the cluster specified by the string; else, the program will insert into the localhost.

This program will create a MongoDB database named 'MapReduce' with the following collections:
1. Person
2. Movie
3. Person_Roles
4. Ratings

This program was written by: Yash Karia


For clustering.py:
Usages:
1. To run k-means for single k: python3 clustering.py --k <value of k> [MongoDB connection string]
2. To see the SSE curve for k = 1 to k = 10: python3 clustering.py --sse [MongoDB connection string]

Provide the following in the command-line arguments:
1. The mode of operation ('--k'/'--sse').
	a. For the single k mode i.e. '--k', provide the value of number of clusters (k).
2. The optional parameter of the MongoDB connection string. If provided, the program will read the data from the cluster specified by the string; else, the program will read from the localhost.

This program will read from the MongoDB database named 'MapReduce'. If the mode of operation is single k, the program will perform k-means clustering based on the value of k and show a scatter plot with different colors for each cluster of data. If the mode of operation is sse, the program will perform k-means clustering on k = 1 to k = 10 and compute the Sum of Squared Errors (SSE) for the clusters for each value of k and will plot the line graph of the SSE against k.

This program was written by: Sri Rachana Achyuthuni with minor modifications by Yash Karia


For frequent_itemset_mining.py:
Usage: python3 frequent_itemset_mining.py <value of minimum support> <maximum allowed size of itemset (0 for no limit)> [MongoDB connection string]

Provide the following in the command-line arguments:
1. The value of the minimum support that is a positive integer for the Apriori algorithm.
2. The maximum allowed size for the frequent itemsets. This should be a positive integer too; if it is 0, there will be no limit on the size.
3. The optional parameter of the MongoDB connection string. If provided, the program will read the data from the cluster specified by the string; else, the program will read from the localhost.

This program was written by: Yash Karia and Dhrumil Mehta


For average_rating_per_user.py:
Usage: python3 average_rating_per_user.py

This program was written by: Sri Rachana Achyuthuni


For movies_per_genre.py:
Usage: python3 movies_per_genre.py

This program was written by: Sri Rachana Achyuthuni


For movies_per_year.py:
Usage: python3 movies_per_year.py

This program was written by: Sri Rachana Achyuthuni


For pairwise_comparison.py:
Usage: python3 pairwise_comparison.py

This program was written by: Dhrumil Mehta