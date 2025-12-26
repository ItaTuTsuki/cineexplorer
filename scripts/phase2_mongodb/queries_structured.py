import pymongo
from pymongo import MongoClient
import time

# Configuration (Doit être la même que migrate_flat.py)
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"
COLLECTION = "movies_complete"

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

# --- REQUÊTES OPTIMISÉES (NoSQL style) ---

def q1_filmography_struct(db, actor_name):
    """Q1: Filmographie"""
    pipeline = [
        {"$match": {
            "cast.name": {"$regex": actor_name, "$options": "i"}
        }},
        {"$project": {
            "title": 1, 
            "year": 1, 
            "rating": "$rating.average",
            "role": {
                "$filter": {
                    "input": "$cast",
                    "as": "c",
                    "cond": {"$regexMatch": {"input": "$$c.name", "regex": actor_name, "options": "i"}}
                }
            }
        }},
        {"$sort": {"year": -1}},
        {"$limit": 20}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q2_top_movies_struct(db, genre, year_min, year_max, limit=5):
    """Q2: Top N films"""
    query = {
        "genres": genre,
        "year": {"$gte": year_min, "$lte": year_max},
        "rating.votes": {"$gt": 1000}
    }
    projection = {"title": 1, "year": 1, "rating": 1, "_id": 0}
    return list(db[COLLECTION].find(query, projection).sort("rating.average", -1).limit(limit))

def q3_multi_roles_struct(db):
    """Q3: Acteurs multi-rôles"""
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {
            "_id": {"mid": "$_id", "pid": "$cast.person_id", "name": "$cast.name"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q4_collaborations_struct(db, actor_name):
    """Q4: Collaborations"""
    pipeline = [
        {"$match": {"cast.name": {"$regex": actor_name, "$options": "i"}}},
        {"$unwind": "$directors"},
        {"$group": {
            "_id": "$directors.name",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q5_popular_genres_struct(db):
    """Q5: Genres populaires"""
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {
            "_id": "$genres",
            "avg_rating": {"$avg": "$rating.average"},
            "count": {"$sum": 1}
        }},
        {"$match": {"avg_rating": {"$gt": 7.0}, "count": {"$gt": 50}}},
        {"$sort": {"avg_rating": -1}},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q6_career_struct(db, actor_name):
    """Q6: Carrière"""
    pipeline = [
        {"$match": {
            "cast.name": {"$regex": actor_name, "$options": "i"},
            "year": {"$ne": None}
        }},
        {"$project": {
            "decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]},
            "rating": "$rating.average"
        }},
        {"$group": {
            "_id": "$decade",
            "count": {"$sum": 1},
            "avg_rating": {"$avg": "$rating"}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q7_rank_genre_struct(db):
    """Q7: Classement"""
    pipeline = [
        {"$match": {"rating.votes": {"$gt": 5000}}},
        {"$unwind": "$genres"},
        {"$setWindowFields": {
            "partitionBy": "$genres",
            "sortBy": {"rating.average": -1},
            "output": {"rank": {"$rank": {}}}
        }},
        {"$match": {"rank": {"$lte": 3}}},
        {"$project": {"genre": "$genres", "title": 1, "rating": "$rating.average", "rank": 1}}
    ]
    try:
        return list(db[COLLECTION].aggregate(pipeline))
    except:
        return [{"error": "Version MongoDB < 5.0"}]

def q8_breakout_struct(db):
    """Q8: Percée"""
    pipeline = [
        {"$match": {"rating.votes": {"$gt": 200000}}},
        {"$unwind": "$cast"},
        {"$sort": {"year": 1}}, 
        {"$group": {
            "_id": "$cast.person_id",
            "first_hit": {"$first": "$title"},
            "votes": {"$first": "$rating.votes"},
            "year": {"$first": "$year"},
            "name": {"$first": "$cast.name"}
        }},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q9_complex_struct(db):
    """Q9: Complexe (Filtrage Region FR)"""
    query = {
        "runtime": {"$gt": 120},
        "rating.average": {"$gt": 8.0},
        "titles": {"$elemMatch": {"region": "FR"}}
    }
    return list(db[COLLECTION].find(query, {"title": 1, "rating": 1}).limit(20))

def run_benchmark():
    db = get_db()
    
    # Vérif simple
    if db[COLLECTION].count_documents({}) == 0:
        print(f"❌ ERREUR: La collection '{COLLECTION}' est VIDE.")
        return

    # Index (au cas où ils n'auraient pas été créés par la migration)
    db[COLLECTION].create_index("cast.name")
    db[COLLECTION].create_index("genres")

    tests = [
        ("Q1 Filmographie", lambda: q1_filmography_struct(db, "Tom Hanks")),
        ("Q2 Top Films", lambda: q2_top_movies_struct(db, "Drama", 1990, 2000)),
        ("Q3 Multi-rôles", lambda: q3_multi_roles_struct(db)),
        ("Q4 Collaborations", lambda: q4_collaborations_struct(db, "Leonardo DiCaprio")),
        ("Q5 Genres Pop", lambda: q5_popular_genres_struct(db)),
        ("Q6 Carrière", lambda: q6_career_struct(db, "Brad Pitt")),
        ("Q7 Classement", lambda: q7_rank_genre_struct(db)),
        ("Q8 Propulsés", lambda: q8_breakout_struct(db)),
        ("Q9 Complexe", lambda: q9_complex_struct(db)),
    ]

    print("\n--- BENCHMARK MONGODB (Documents Structurés) ---")
    print(f"{'Requête':<20} | {'Temps (ms)':<10} | {'Résultat (ex)'}")
    print("-" * 65)

    for name, func in tests:
        start = time.time()
        try:
            res = func()
            duration = (time.time() - start) * 1000
            sample = str(res[0])[:40] + "..." if res else "Aucun résultat"
            print(f"{name:<20} | {duration:<10.2f} | {sample}")
        except Exception as e:
            print(f"{name:<20} | ERREUR     | {str(e)[:40]}")

if __name__ == "__main__":
    run_benchmark()