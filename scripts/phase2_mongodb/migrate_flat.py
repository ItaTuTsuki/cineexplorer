import sqlite3
import pymongo
import os
import time

# --- CONFIGURATION ---
# On remonte de 3 niveaux pour trouver le fichier data/imdb.db
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SQLITE_DB = os.path.join(BASE_DIR, 'data', 'imdb.db')
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb_flat"

# Liste des tables a migrer
TABLES = [
    "movies", "persons", "ratings", "genres", 
    "titles", "principals", "directors", "writers", 
    "characters", "professions"
]

def clean_orphans(conn, table_name, fk_col, parent_table, parent_pk):
    """Fonction utilitaire pour supprimer les orphelins SQLite"""
    cursor = conn.cursor()
    
    # On verifie s'il y a des orphelins
    query_count = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE {fk_col} NOT IN (SELECT {parent_pk} FROM {parent_table})
    """
    cursor.execute(query_count)
    count = cursor.fetchone()[0]
    
    if count > 0:
        print(f"Reparation SQLite : Suppression de {count} orphelins dans '{table_name}'...")
        query_delete = f"""
            DELETE FROM {table_name}
            WHERE {fk_col} NOT IN (SELECT {parent_pk} FROM {parent_table})
        """
        cursor.execute(query_delete)
        conn.commit()

def repair_sqlite_database(conn):
    """Lance toutes les verifications d'integrite sur SQLite"""
    print("Verification et reparation de la base SQLite...")
    
    # 1. Nettoyage des tables liees aux Personnes
    clean_orphans(conn, "writers", "person_id", "persons", "person_id")
    clean_orphans(conn, "directors", "person_id", "persons", "person_id")
    clean_orphans(conn, "principals", "person_id", "persons", "person_id")
    clean_orphans(conn, "characters", "person_id", "persons", "person_id")
    
    # 2. Nettoyage des tables liees aux Films
    clean_orphans(conn, "ratings", "movie_id", "movies", "movie_id")
    clean_orphans(conn, "titles", "movie_id", "movies", "movie_id")
    clean_orphans(conn, "genres", "movie_id", "movies", "movie_id")
    
    print("Base SQLite verifiee et integre.\n")

def create_indexes(db):
    """Cree les index MongoDB indispensables"""
    print("\nCreation des index optimises sur MongoDB...")
    
    # Index simples sur cles etrangeres
    for collection in ["movies", "ratings", "genres", "titles", "principals", "directors", "writers", "characters"]:
        db[collection].create_index("movie_id")
        
    for collection in ["persons", "principals", "directors", "writers", "characters", "professions"]:
        db[collection].create_index("person_id")

    # Index composites et texte
    db.principals.create_index([("movie_id", 1), ("person_id", 1)])
    db.characters.create_index([("movie_id", 1), ("person_id", 1)])
    db.persons.create_index("name")
    db.genres.create_index("genre")
    
    print("Index crees avec succes.")

def migrate_flat():
    # 1. Connexion SQLite
    if not os.path.exists(SQLITE_DB):
        print(f"Erreur : Base SQLite introuvable ({SQLITE_DB})")
        return

    print(f"Connexion a SQLite : {SQLITE_DB}")
    conn_sql = sqlite3.connect(SQLITE_DB)
    conn_sql.row_factory = sqlite3.Row 
    cursor_sql = conn_sql.cursor()

    # --- ETAPE DE REPARATION ---
    repair_sqlite_database(conn_sql)
    # ---------------------------

    # 2. Connexion MongoDB
    print(f"Connexion a MongoDB : {MONGO_URI}")
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
    except Exception as e:
        print(f"Erreur connexion MongoDB : {e}")
        return

    # Nettoyage complet MongoDB
    print(f"Reinitialisation de la base MongoDB '{DB_NAME}'...")
    client.drop_database(DB_NAME)

    start_global = time.time()

    # 3. Migration des donnees
    for table in TABLES:
        print(f"Migration de la table '{table}'...", end=" ")
        
        cursor_sql.execute(f"SELECT * FROM {table}")
        rows = cursor_sql.fetchall()
        
        if not rows:
            print("Vide.")
            continue

        documents = [dict(row) for row in rows]
        
        if documents:
            db[table].insert_many(documents)
            print(f"{len(documents)} docs.")

    # 4. Creation des index
    create_indexes(db)

    conn_sql.close()
    client.close()
    print(f"\nMIGRATION TERMINEE en {time.time() - start_global:.2f} secondes.")

if __name__ == "__main__":
    migrate_flat()