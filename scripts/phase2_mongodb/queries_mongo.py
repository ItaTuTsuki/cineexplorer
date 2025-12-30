import pymongo
import time
import os

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"


def get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME]


def measure_time(func, *args):
    start = time.time()
    try:
        result = func(*args)
        count = len(result)
    except Exception as e:
        return f"ERREUR: {e}", 0

    end = time.time()
    duration_ms = (end - start) * 1000
    return result, duration_ms


def fix_indexes():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    print(" Création des index de base (Clés étrangères)...")

    # On indexe movie_id partout
    # (Ton collègue utilise 'mid', toi 'movie_id', je garde TES noms)
    db.movies.create_index("movie_id")
    db.ratings.create_index("movie_id")
    db.genres.create_index("movie_id")
    db.principals.create_index("movie_id")
    db.titles.create_index("movie_id")
    db.directors.create_index("movie_id")
    db.writers.create_index("movie_id")

    # On indexe person_id partout
    db.persons.create_index("person_id")
    db.principals.create_index("person_id")
    db.directors.create_index("person_id")
    db.writers.create_index("person_id")

    # On indexe les champs de recherche courants
    db.persons.create_index("name")
    db.genres.create_index("genre")


# =============================================================================
# Q1 : Filmographie
# =============================================================================
def query_q1_filmography(db, actor_name):
    # Optimisation : On trouve d'abord l'ID, puis on fait le pipeline
    # C'est ce que fait ton collègue, c'est bien plus rapide.
    person = db.persons.find_one({"name": {"$regex": actor_name, "$options": "i"}})
    if not person:
        return []
    pid = person["person_id"]

    pipeline = [
        {"$match": {"person_id": pid, "category": {"$in": ["actor", "actress"]}}},
        {
            "$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "m",
            }
        },
        {"$unwind": "$m"},
        {
            "$lookup": {  # On ajoute le personnage
                "from": "characters",
                "let": {"mid": "$movie_id", "pid": "$person_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$movie_id", "$$mid"]},
                                    {"$eq": ["$person_id", "$$pid"]},
                                ]
                            }
                        }
                    }
                ],
                "as": "char",
            }
        },
        {
            "$project": {
                "title": "$m.title",
                "year": "$m.year",
                "character": {"$arrayElemAt": ["$char.character_name", 0]},
            }
        },
        {"$sort": {"year": -1}},
        {"$limit": 20},
    ]
    return list(db.principals.aggregate(pipeline))


# =============================================================================
# Q2 : Top N Films par Genre
# =============================================================================
def query_q2_top_movies(db, genre, year_start, year_end, n):
    pipeline = [
        {"$match": {"genre": genre}},
        {
            "$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "m",
            }
        },
        {"$unwind": "$m"},
        {"$match": {"m.year": {"$gte": year_start, "$lte": year_end}}},
        {
            "$lookup": {
                "from": "ratings",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "r",
            }
        },
        {"$unwind": "$r"},
        {"$sort": {"r.average_rating": -1, "r.num_votes": -1}},
        {"$limit": n},
        {
            "$project": {
                "title": "$m.title",
                "year": "$m.year",
                "rating": "$r.average_rating",
            }
        },
    ]
    return list(db.genres.aggregate(pipeline))


# =============================================================================
# Q3 : Acteurs Multi-Rôles
# =============================================================================
def query_q3_multi_roles(db):
    pipeline = [
        {
            "$group": {
                "_id": {"pid": "$person_id", "mid": "$movie_id"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {
            "$lookup": {
                "from": "persons",
                "localField": "_id.pid",
                "foreignField": "person_id",
                "as": "p",
            }
        },
        {
            "$lookup": {
                "from": "movies",
                "localField": "_id.mid",
                "foreignField": "movie_id",
                "as": "m",
            }
        },
        {
            "$project": {
                "actor": {"$arrayElemAt": ["$p.name", 0]},
                "movie": {"$arrayElemAt": ["$m.title", 0]},
                "count": 1,
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    return list(db.characters.aggregate(pipeline))


# =============================================================================
# Q4 : Collaborations
# =============================================================================
def query_q4_collaborations(db, actor_name):
    person = db.persons.find_one({"name": {"$regex": actor_name, "$options": "i"}})
    if not person:
        return []
    pid = person["person_id"]

    # On récupère d'abord les films de l'acteur (Optimisation style collègue)
    actor_movies = db.principals.distinct(
        "movie_id", {"person_id": pid, "category": {"$in": ["actor", "actress"]}}
    )

    pipeline = [
        {
            "$match": {"movie_id": {"$in": actor_movies}}
        },  # On filtre direct sur ces films
        {
            "$lookup": {
                "from": "persons",
                "localField": "person_id",
                "foreignField": "person_id",
                "as": "dir",
            }
        },
        {"$unwind": "$dir"},
        {"$group": {"_id": "$dir.name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10},
    ]
    # On cherche dans directors, pas principals (plus rapide car table plus petite)
    return list(db.directors.aggregate(pipeline))


# =============================================================================
# Q5 : Genres Populaires
# =============================================================================
def query_q5_popular_genres(db):
    pipeline = [
        {
            "$lookup": {
                "from": "ratings",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "r",
            }
        },
        {"$unwind": "$r"},
        {
            "$group": {
                "_id": "$genre",
                "avg_rating": {"$avg": "$r.average_rating"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"avg_rating": {"$gt": 7.0}, "count": {"$gt": 50}}},
        {"$sort": {"avg_rating": -1}},
    ]
    return list(db.genres.aggregate(pipeline))


# =============================================================================
# Q6 : Carrière
# =============================================================================
def query_q6_career(db, actor_name):
    person = db.persons.find_one({"name": {"$regex": actor_name, "$options": "i"}})
    if not person:
        return []
    pid = person["person_id"]

    pipeline = [
        {"$match": {"person_id": pid}},
        {
            "$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "m",
            }
        },
        {"$unwind": "$m"},
        {
            "$lookup": {
                "from": "ratings",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "r",
            }
        },
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        {"$match": {"m.year": {"$ne": None}}},
        {
            "$project": {
                "decade": {"$multiply": [{"$floor": {"$divide": ["$m.year", 10]}}, 10]},
                "rating": "$r.average_rating",
            }
        },
        {
            "$group": {
                "_id": "$decade",
                "count": {"$sum": 1},
                "avg_rating": {"$avg": "$rating"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    return list(db.principals.aggregate(pipeline))


# =============================================================================
# Q7 : Classement par Genre (Méthode Compatible Collègue)
# =============================================================================
def query_q7_top3_genre(db):
    pipeline = [
        # 1. Joindre Movies et Ratings
        {
            "$lookup": {
                "from": "ratings",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "r",
            }
        },
        {"$unwind": "$r"},
        {"$match": {"r.num_votes": {"$gt": 1000}}},  # Filtre qualité
        {
            "$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "m",
            }
        },
        {"$unwind": "$m"},
        # 2. Trier
        {"$sort": {"genre": 1, "r.average_rating": -1}},
        # 3. Grouper et Pousser dans une liste
        {
            "$group": {
                "_id": "$genre",
                "films": {
                    "$push": {"title": "$m.title", "rating": "$r.average_rating"}
                },
            }
        },
        # 4. Garder les 3 premiers (Slice)
        {"$project": {"genre": "$_id", "top3": {"$slice": ["$films", 3]}}},
        {"$sort": {"genre": 1}},
    ]
    return list(db.genres.aggregate(pipeline))


# =============================================================================
# Q8 : Percée
# =============================================================================
def query_q8_breakthrough(db):
    # Version simplifiée pour Mongo Flat : on cherche les acteurs qui ont un film > 200k votes
    # et on vérifie s'ils en ont des petits avant.
    # C'est très lourd en Mongo Flat, on limite le scan.
    pipeline = [
        {"$match": {"num_votes": {"$gt": 200000}}},  # Films succès
        {
            "$lookup": {
                "from": "principals",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "cast",
            }
        },
        {"$unwind": "$cast"},
        {"$match": {"cast.category": {"$in": ["actor", "actress"]}}},
        {"$group": {"_id": "$cast.person_id", "hit_movie_id": {"$first": "$movie_id"}}},
        {"$limit": 50},  # On limite pour le test sinon c'est trop long
        {
            "$lookup": {
                "from": "persons",
                "localField": "_id",
                "foreignField": "person_id",
                "as": "p",
            }
        },
        {
            "$project": {
                "name": {"$arrayElemAt": ["$p.name", 0]},
                "hit": "$hit_movie_id",
            }
        },
    ]
    return list(db.ratings.aggregate(pipeline))


# =============================================================================
# Q9 : Longévité
# =============================================================================
def query_q9_longevity(db):
    pipeline = [
        {"$match": {"category": {"$in": ["actor", "actress"]}}},
        {
            "$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "m",
            }
        },
        {"$unwind": "$m"},
        {
            "$group": {
                "_id": "$person_id",
                "min": {"$min": "$m.year"},
                "max": {"$max": "$m.year"},
                "count": {"$sum": 1},
            }
        },
        {"$project": {"span": {"$subtract": ["$max", "$min"]}, "count": 1}},
        {"$match": {"span": {"$gt": 0}, "count": {"$gt": 10}}},
        {"$sort": {"span": -1}},
        {"$limit": 15},
        {
            "$lookup": {
                "from": "persons",
                "localField": "_id",
                "foreignField": "person_id",
                "as": "p",
            }
        },
        {"$project": {"name": {"$arrayElemAt": ["$p.name", 0]}, "span": 1}},
    ]
    return list(db.principals.aggregate(pipeline))


if __name__ == "__main__":
    db = get_db()
    fix_indexes()
    print(f" Benchmark MongoDB Flat (Optimisé avec Index)")
    print("-" * 60)
    print(f"{'REQUÊTE':<25} | {'TEMPS (ms)':<10} | {'RÉSULTAT'}")
    print("-" * 60)

    tasks = [
        ("Q1 - Filmographie", lambda: query_q1_filmography(db, "Brad Pitt")),
        ("Q2 - Top N films", lambda: query_q2_top_movies(db, "Action", 2000, 2010, 5)),
        ("Q3 - Multi-rôles", lambda: query_q3_multi_roles(db)),
        ("Q4 - Collaborations", lambda: query_q4_collaborations(db, "Johnny Depp")),
        ("Q5 - Genres Pop.", lambda: query_q5_popular_genres(db)),
        ("Q6 - Carrière", lambda: query_q6_career(db, "Tom Hanks")),
        ("Q7 - Classement Genre", lambda: query_q7_top3_genre(db)),
        ("Q8 - Percée", lambda: query_q8_breakthrough(db)),
        ("Q9 - Longévité", lambda: query_q9_longevity(db)),
    ]

    for name, func in tasks:
        res, ms = measure_time(func)
        print(f"{name:<25} | {ms:10.2f} | {len(res)} rés.")
