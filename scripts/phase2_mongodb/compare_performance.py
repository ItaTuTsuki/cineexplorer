import pymongo
import time
import random

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"

def get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME]

def get_movie_flat_method(db, movie_id):
    """
    Récupération "Flat" : on simule des jointures manuelles coté applicatif.
    Nécessite plusieurs requêtes successives.
    """
    # 1. Infos film
    movie = db.movies.find_one({"movie_id": movie_id})
    if not movie: return None
    
    # 2. Ratings
    rating = db.ratings.find_one({"movie_id": movie_id})
    
    # 3. Genres
    genres = list(db.genres.find({"movie_id": movie_id}))
    
    # 4. Titres
    titles = list(db.titles.find({"movie_id": movie_id}))
    
    # 5. Réalisateurs
    directors = []
    for d in db.directors.find({"movie_id": movie_id}):
        person = db.persons.find_one({"person_id": d["person_id"]})
        if person:
            directors.append(person["name"])
            
    # 6. Scénaristes
    writers = []
    for w in db.writers.find({"movie_id": movie_id}):
        person = db.persons.find_one({"person_id": w["person_id"]})
        if person:
            writers.append(person["name"])
            
    # 7. Casting (boucle sur chaque acteur pour avoir son nom)
    cast = []
    principals = db.principals.find({"movie_id": movie_id, "category": {"$in": ["actor", "actress"]}}).limit(10)
    for p in principals:
        person = db.persons.find_one({"person_id": p["person_id"]})
        if person:
            cast.append({
                "name": person["name"],
                "role": p.get("job")
            })
            
    return {
        "title": movie.get("title"),
        "cast_size": len(cast)
    }

def get_movie_structured_method(db, movie_id):
    """
    Récupération "Structured" : une seule lecture par ID.
    """
    return db.movies_complete.find_one({"_id": movie_id})

def run_benchmark():
    db = get_db()
    
    # On récupère 50 IDs au hasard pour le test
    print("Sélection d'un échantillon de films...")
    sample_ids = [
        doc["_id"] for doc in db.movies_complete.aggregate([
            {"$match": {"year": {"$gt": 2000}}}, # Filtre sur films récents
            {"$sample": {"size": 50}}
        ])
    ]
    
    if not sample_ids:
        print("Erreur : la collection movies_complete est vide.")
        return

    print(f"Benchmark sur {len(sample_ids)} films...")
    print("-" * 60)

    # --- Test Flat ---
    start_flat = time.time()
    for mid in sample_ids:
        get_movie_flat_method(db, mid)
    end_flat = time.time()
    avg_flat = ((end_flat - start_flat) / len(sample_ids)) * 1000

    # --- Test Structured ---
    start_struct = time.time()
    for mid in sample_ids:
        get_movie_structured_method(db, mid)
    end_struct = time.time()
    avg_struct = ((end_struct - start_struct) / len(sample_ids)) * 1000

    # --- Résultats ---
    print(f"{'METHODE':<20} | {'TEMPS MOYEN (ms)':<20} | {'REQUETES/FILM'}")
    print("-" * 60)
    print(f"{'FLAT':<20} | {avg_flat:<20.4f} | ~10")
    print(f"{'STRUCTURED':<20} | {avg_struct:<20.4f} | 1")
    print("-" * 60)
    
    if avg_struct > 0:
        ratio = avg_flat / avg_struct
        print(f"GAIN : x{ratio:.1f}")

if __name__ == "__main__":
    run_benchmark()