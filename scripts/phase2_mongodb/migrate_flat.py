import sqlite3
import pymongo
import os
import time

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SQLITE_DB = os.path.join(BASE_DIR, "data", "imdb.db")
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"

TABLES = [
    "movies",
    "persons",
    "ratings",
    "genres",
    "titles",
    "principals",
    "directors",
    "writers",
    "characters",
    "professions",
]


def get_corrected_doc(doc):
    # On renomme les cles pour etre coherent avec la suite du projet
    new_doc = {}
    for k, v in doc.items():
        if k == "mid":
            new_doc["movie_id"] = v
        elif k == "pid":
            new_doc["person_id"] = v
        else:
            new_doc[k] = v
    return new_doc


def migrate_flat():
    if not os.path.exists(SQLITE_DB):
        print(f"Erreur : Base SQLite introuvable ({SQLITE_DB})")
        return

    print(f"Connexion a SQLite : {SQLITE_DB}")
    conn_sql = sqlite3.connect(SQLITE_DB)
    conn_sql.row_factory = sqlite3.Row
    cursor_sql = conn_sql.cursor()

    print(f"Connexion a MongoDB : {DB_NAME}")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # On vide la base pour repartir de zero
    client.drop_database(DB_NAME)

    start_global = time.time()

    for table in TABLES:
        print(f"Migration de '{table}'...", end=" ")
        cursor_sql.execute(f"SELECT * FROM {table}")
        rows = cursor_sql.fetchall()

        if not rows:
            print("Vide.")
            continue

        # Conversion et correction des noms de colonnes
        documents = [get_corrected_doc(dict(row)) for row in rows]

        if documents:
            db[table].insert_many(documents)
            print(f"{len(documents)} documents inseres")

    # Creation des index
    print("\nCreation des index...")
    for col in TABLES:
        if col in ["persons", "professions"]:
            continue
        db[col].create_index("movie_id")

    for col in ["persons", "principals", "directors", "writers", "characters"]:
        db[col].create_index("person_id")

    db.persons.create_index("name")
    db.genres.create_index("genre")

    print(f"\nMigration terminee en {time.time() - start_global:.2f}s")


if __name__ == "__main__":
    migrate_flat()
