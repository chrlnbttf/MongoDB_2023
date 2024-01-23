# fonctionne
pprint(list(books.aggregate([
    {"$match":{"_id":"1"}},
    {"$project": {
        "authors":1,
        "category_1": {"$arrayElemAt": ["$categories", 0]},
        "category_2": {"$arrayElemAt": ["$categories", 1]}
    }}
])))

# fonctionne
print('le nombre de publiés et non publiés est :',\
       list(books.aggregate([
    {"$group" : {"_id" : "$status", "nb": {"$sum":1}}} ])))

# fonctionne
print('le nombre de page maximum par status est :',\
       list(books.aggregate([
    {"$group" : {"_id" : "$status", "max_pages" : {"$max":"$pageCount"}}} ])))

# fonctionne, mais pas l'affichage de la stat du sous-champ 2ème élément de categories
print('nb de pages maximum par catégorie 1 est :',\
      list(books.aggregate([
      {"$project" : {"category_1" : { "$arrayElemAt": [ "$categories", 0 ] }}},
      {"$group" : {"_id" : "$category_1", "max_pages" : {"$max":"$pageCount"},\
      "min_pages" : {"$min":"$pageCount"}, "moy_pages" : {"$avg":"$pageCount"} }} ])) )

# how to remove empty lines
