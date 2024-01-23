from pymongo import MongoClient 

from pprint import pprint 

import re

from datetime import datetime

host="localhost",
port = 27017

client = MongoClient(
    host="127.0.0.1",
    port = 27017,
    username='admin',
    password='pass'
)

print("la liste des bases de données est :", client.list_database_names())
print('\n')

sample = client["sample"]
print("la liste des collections est :", sample.list_collection_names())
print('\n')

books = client["sample"]["books"]

print('\n')

print("un exemple au hasard de livre est :")
pprint(books.find_one())
print('\n')

print("un 2ème exemple correspondant à l'id 2 de livre est :")
pprint(list(books.find({"_id":2})))
print('\n')

print("le nombre de documents de la collection est :", books.count_documents({}))
print('\n')

print("le nombre de livres de plus de 400 pages est :", \
      len(list(books.find({"pageCount":{"$gt":400}}))))
print('\n')

print("le nombre de livres de plus de 400 pages publiés est :", \
      len(list(books.find({"$and":[ {"pageCount":{"$gt":400}}, {"status": "PUBLISH"} ]}))))
print('\n')

exp = re.compile("Android")
print("le nombre de livres où Android est présent dans une des descriptions est : ",\
      len(list(books.find( {"$or":[ {"shortDescription":exp}, {"books.longDescription":exp}]}))))
print('\n')

print("les modalités du 1er élément des catégories sont :")
category_1 = list(books.distinct("categories.0"))
pprint(category_1)
print('\n')

print("les modalités du 2ème élément des catégories sont :")
category_2 = list(books.aggregate([
    {"$project" : {"category_2" : { "$arrayElemAt": [ "$categories", 1 ] }}},
    {"$group" : {"_id" : "$category_2"}}
]))
pprint(category_2)
print('\n')

exp = re.compile(r'Python|Java|C\+\+|ScalaPython')
print("le nombre de livre contenant les mots-clefs Python, Java, C++, Scala et Python dans leur description longue : ", \
      len(list(books.find({"longDescription": {"$regex": exp}}))))
print('\n')

print('les statistiques relatives aux pages des livres par catégorie sont :')
pprint(list(books.aggregate([
    {"$group" : {"_id" : "$categories", "max_pages" : {"$max":"$pageCount"},\
      "min_pages" : {"$min":"$pageCount"}, "moy_pages" : {"$avg":"$pageCount"} }} ])) )
print('\n')

print('les dates de publication avec annee, mois et jour dans des colonnes separees sont :')
pprint(list(books.aggregate([
      {"$project":{
            "_id": 0,
            "year": {"$year": "$publishedDate"},
            "month": {"$month": "$publishedDate"},
            "day": {"$dayOfMonth": "$publishedDate"}}},
      {"$match":
            {"year":{"$gte":2009}}}
])))
print('\n')

print('les auteurs de chaque livre notés dans des colonnes séparées sont : ')
pprint(list(books.aggregate([
      {"$project" : {
          "_id":0,
          "author1":{"$arrayElemAt":["$authors", 0]},
          "author2":{"$arrayElemAt":["$authors", 1]},
          "author3":{"$arrayElemAt":["$authors", 2]}
      }}
])))
# ArrayElemAt remplace les espaces vides par des string vides
print('\n')

print("le nombre de livres écrit par auteurs notés en premier dans la liste est :")
pprint(list(books.aggregate([
      {"$project":
       {"_id":0,
          "author1":{"$arrayElemAt":["$authors", 0]}}},
      {"$group":
       {"_id":"$author1", "nb":{"$sum":1}}},
      {"$sort":
       {"nb":-1}},
      {"$limit": 10}
])))
print('\n')

print("la distribution du nombre d'auteurs est :")
pprint(list(books.aggregate([
        {"$project" : {
            "_id":0,
            "nb_auteur": {"$size":"$authors"}}},
        {"$count" : "nb_auteur"}
])))
print('\n')