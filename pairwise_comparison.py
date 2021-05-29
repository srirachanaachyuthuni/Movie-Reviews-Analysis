import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

connection = MongoClient()

db = connection.MapReduce
movies = db.Movie
movies_df = pd.DataFrame(list(movies.find()))

movies_df_subset = movies_df[["runtimeMinutes", "startYear"]]

plt.figure(figsize=(10, 8))
plt.scatter(movies_df_subset["startYear"], movies_df_subset["runtimeMinutes"])
plt.title("Runtime minutes w.r.t. Years released")
plt.ylabel("Runtime minutes")
plt.xlabel("Year")
plt.show()

movies_df_subset = movies_df_subset.sample(n=1000, random_state=100)
plt.figure(figsize=(10, 8))
plt.scatter(movies_df_subset["startYear"], movies_df_subset["runtimeMinutes"], color="orange")
plt.title("Runtime minutes w.r.t. Years released")
plt.xlabel("Year")
plt.ylabel("Runtime minutes")
plt.show()

db = connection.MapReduce
persons = db.Person
person_df = pd.DataFrame(list(persons.find()))
person_df_subset = person_df[["birthYear", "deathYear"]]

person_df_subset = person_df_subset.sample(n=1000, random_state=7)

plt.figure(figsize=(10, 8))
plt.scatter(person_df_subset["birthYear"], person_df_subset["deathYear"], color="green")
plt.title("Birthyear v/s Deathyear of Persons")
plt.xlabel("BirthYear")
plt.ylabel("DeathYear")
plt.show()
