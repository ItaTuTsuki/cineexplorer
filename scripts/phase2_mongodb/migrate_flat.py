import sqlite3
import pymongo
import os
import time

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SQLITE_DB = os.path.join(BASE_DIR, 'data', 'imdb.db')
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"

# Liste des tables à migrer
TABLES = [
    "movies", "persons", "ratings", "genres", 
    "titles", "principals", "directors", "writers", 
    "characters", "professions"
]

def create_indexes(db):
    """Cree les index indispensables pour les performances (T2.3 et T2.4)"""
    print("\nCreation des index optimises...")
    
    # 1. Index sur les cles etrangeres (JOINs)
    # On utilise "movie_id" et "person_id" car c'est ce qui vient de ton SQLite
    for collection in ["movies", "ratings", "genres", "titles", "principals", "directors", "writers", "characters"]:
        db[collection].create_index("movie_id")
        
    for collection in ["persons", "principals", "directors", "writers", "characters", "professions"]:
        db[collection].create_index("person_id")

    # 2. Index composites pour les tables de liaison (Optimisation specifique)
    db.principals.create_index([("movie_id", 1), ("person_id", 1)])
    db.characters.create_index([("movie_id", 1), ("person_id", 1)])

    # 3. Index de recherche textuelle (pour les WHERE name LIKE ...)
    db.persons.create_index("name")
    db.genres.create_index("genre")
    
    print("  Index crees avec succes.")

def migrate_flat():
    # 1. Connexion SQLite
    if not os.path.exists(SQLITE_DB):
        print(f"Erreur : Base SQLite introuvable ({SQLITE_DB})")
        return

    print(f"Connexion a SQLite : {SQLITE_DB}")
    conn_sql = sqlite3.connect(SQLITE_DB)
    conn_sql.row_factory = sqlite3.Row 
    cursor_sql = conn_sql.cursor()

    # 2. Connexion MongoDB
    print(f"Connexion a MongoDB : {MONGO_URI}")
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
    except Exception as e:
        print(f"Erreur connexion MongoDB : {e}")
        return

    # Nettoyage complet
    print("Nettoyage de la base MongoDB existante...")
    client.drop_database(DB_NAME)

    start_global = time.time()

    # 3. Migration des donnees
    for table in TABLES:
        print(f"Migration de la table '{table}'...")
        
        # A. Lecture SQLite
        cursor_sql.execute(f"SELECT * FROM {table}")
        rows = cursor_sql.fetchall()
        
        if not rows:
            print(f"  Table vide, ignoree.")
            continue

        # B. Conversion en dictionnaires
        documents = [dict(row) for row in rows]
        
        # C. Insertion MongoDB
        if documents:
            db[table].insert_many(documents)
            print(f"  {len(documents)} documents inseres.")

    # 4. CREATION DES INDEX
    create_indexes(db)

    conn_sql.close()
    client.close()
    print(f"\nMIGRATION TERMINEE en {time.time() - start_global:.2f} secondes.")

if __name__ == "__main__":
    migrate_flat()