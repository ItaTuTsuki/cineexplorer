import pymongo
import time
import random

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"

def get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME]

def get_movie_flat(db, movie_id):
    # On simule les jointures manuelles du modele relationnel
    movie = db.movies.find_one({"movie_id": movie_id})
    if not movie: return None
    
    rating = db.ratings.find_one({"movie_id": movie_id})
    genres = list(db.genres.find({"movie_id": movie_id}))
    titles = list(db.titles.find({"movie_id": movie_id}))
    
    directors = []
    for d in db.directors.find({"movie_id": movie_id}):
        person = db.persons.find_one({"person_id": d["person_id"]})
        if person: directors.append(person["name"])
            
    writers = []
    for w in db.writers.find({"movie_id": movie_id}):
        person = db.persons.find_one({"person_id": w["person_id"]})
        if person: writers.append(person["name"])
            
    cast = []
    principals = db.principals.find({"movie_id": movie_id, "category": {"$in": ["actor", "actress"]}}).limit(10)
    for p in principals:
        person = db.persons.find_one({"person_id": p["person_id"]})
        if person:
            cast.append({"name": person["name"], "role": p.get("job")})
            
    return {"title": movie.get("title"), "cast_size": len(cast)}

def get_movie_structured(db, movie_id):
    # Une seule lecture directe par ID
    return db.movies_complete.find_one({"_id": movie_id})

def check_storage_size(db):
    print("\n--- COMPARAISON STOCKAGE ---")
    
    flat_colls = ["movies", "ratings", "genres", "principals", "directors", "writers", "titles", "persons", "characters"]
    
    flat_size = 0
    for coll in flat_colls:
        try:
            stats = db.command("collstats", coll)
            flat_size += stats["storageSize"] + stats["totalIndexSize"]
        except: pass
        
    try:
        stats_struct = db.command("collstats", "movies_complete")
        struct_size = stats_struct["storageSize"] + stats_struct["totalIndexSize"]
    except:
        struct_size = 0
        
    print(f"{'Modele':<20} | {'Taille (MB)':<15}")
    print("-" * 40)
    print(f"{'Flat':<20} | {flat_size / (1024*1024):<15.2f}")
    print(f"{'Structure':<20} | {struct_size / (1024*1024):<15.2f}")
    
    if flat_size > 0:
        diff = ((struct_size - flat_size) / flat_size) * 100
        print("-" * 40)
        print(f"Difference : {diff:+.1f}%")

def run_benchmark():
    db = get_db()
    
    # On prend 50 films recents au hasard
    print("Selection de 50 films pour le test...")
    sample_ids = [
        doc["_id"] for doc in db.movies_complete.aggregate([
            {"$match": {"year": {"$gt": 2000}}}, 
            {"$sample": {"size": 50}} 
        ])
    ]
    
    if not sample_ids:
        print("Erreur : collection vide.")
        return

    print(f"Test de performance (Lecture complete)...")
    print("-" * 60)

    # Test Flat
    start = time.time()
    for mid in sample_ids:
        get_movie_flat(db, mid)
    avg_flat = ((time.time() - start) / len(sample_ids)) * 1000

    # Test Structured
    start = time.time()
    for mid in sample_ids:
        get_movie_structured(db, mid)
    avg_struct = ((time.time() - start) / len(sample_ids)) * 1000

    print(f"{'METHODE':<20} | {'TEMPS MOYEN (ms)':<20} | {'REQUETES/FILM'}")
    print("-" * 60)
    print(f"{'FLAT':<20} | {avg_flat:<20.4f} | ~10")
    print(f"{'STRUCTURED':<20} | {avg_struct:<20.4f} | 1")
    print("-" * 60)
    
    if avg_struct > 0:
        print(f"GAIN : x{avg_flat / avg_struct:.1f}")

    check_storage_size(db)

if __name__ == "__main__":
    run_benchmark()