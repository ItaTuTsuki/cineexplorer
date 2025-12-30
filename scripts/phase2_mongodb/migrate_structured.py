import pymongo
import time

# Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"
SOURCE_COLL = "movies"
TARGET_COLL = "movies_complete"


def migrate_structured():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Verification simple
    if db[SOURCE_COLL].count_documents({}) == 0:
        print("Erreur : La collection source est vide. Lancez migrate_flat.py d'abord.")
        return

    print(f"Denormalisation : {SOURCE_COLL} -> {TARGET_COLL}...")
    start_time = time.time()

    # Pipeline d'agregation pour creer le document complet
    pipeline = [
        # 1. Notes
        {
            "$lookup": {
                "from": "ratings",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "r",
            }
        },
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        # 2. Genres
        {
            "$lookup": {
                "from": "genres",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "g",
            }
        },
        # 3. Titres
        {
            "$lookup": {
                "from": "titles",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "t",
            }
        },
        # 4. Realisateurs
        {
            "$lookup": {
                "from": "directors",
                "let": {"mid": "$movie_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                    {
                        "$lookup": {
                            "from": "persons",
                            "localField": "person_id",
                            "foreignField": "person_id",
                            "as": "p",
                        }
                    },
                    {"$unwind": "$p"},
                    {"$project": {"_id": 0, "person_id": 1, "name": "$p.name"}},
                ],
                "as": "dir",
            }
        },
        # 5. Scenaristes
        {
            "$lookup": {
                "from": "writers",
                "let": {"mid": "$movie_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                    {
                        "$lookup": {
                            "from": "persons",
                            "localField": "person_id",
                            "foreignField": "person_id",
                            "as": "p",
                        }
                    },
                    {"$unwind": "$p"},
                    {"$project": {"_id": 0, "person_id": 1, "name": "$p.name"}},
                ],
                "as": "wri",
            }
        },
        # 6. Casting (Acteurs)
        {
            "$lookup": {
                "from": "principals",
                "let": {"mid": "$movie_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$movie_id", "$$mid"]},
                                    {"$in": ["$category", ["actor", "actress"]]},
                                ]
                            }
                        }
                    },
                    {
                        "$lookup": {
                            "from": "persons",
                            "localField": "person_id",
                            "foreignField": "person_id",
                            "as": "p",
                        }
                    },
                    {"$unwind": "$p"},
                    {
                        "$lookup": {
                            "from": "characters",
                            "let": {"pid": "$person_id", "mid": "$movie_id"},
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
                            "_id": 0,
                            "person_id": 1,
                            "name": "$p.name",
                            "ordering": 1,
                            "characters": "$char.character_name",
                        }
                    },
                    {"$sort": {"ordering": 1}},
                ],
                "as": "cast",
            }
        },
        # 7. Structure finale du document
        {
            "$project": {
                "_id": "$movie_id",
                "title": "$title",
                "original_title": "$original_title",
                "year": "$year",
                "runtime": "$runtime",
                "genres": "$g.genre",
                "rating": {"average": "$r.average_rating", "votes": "$r.num_votes"},
                "directors": "$dir",
                "writers": "$wri",
                "cast": "$cast",
                "titles": {
                    "$map": {
                        "input": "$t",
                        "as": "tmp",
                        "in": {"region": "$$tmp.region", "title": "$$tmp.title"},
                    }
                },
            }
        },
        # 8. Sauvegarde
        {"$out": TARGET_COLL},
    ]

    try:
        # allowDiskUse necessaire pour les grosses operations de tri
        db[SOURCE_COLL].aggregate(pipeline, allowDiskUse=True)

        duration = time.time() - start_time
        count = db[TARGET_COLL].count_documents({})

        print(f"Succes. Collection '{TARGET_COLL}' creee avec {count} documents.")
        print(f"Temps total : {duration:.2f} secondes")

        # Index pour la recherche
        print("Creation de l'index sur le titre...")
        db[TARGET_COLL].create_index("title")

    except Exception as e:
        print(f"Erreur : {e}")


if __name__ == "__main__":
    migrate_structured()
