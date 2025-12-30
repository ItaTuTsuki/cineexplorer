# T1.4: Indexation et benchmark
import sqlite3
import time
import os
import sys

# Ajout du chemin racine pour importer le module queries
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from scripts.phase1_sqlite import queries

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "imdb.db")

# --- LISTE DES INDEX À CRÉER ---
INDEXES = [
    # Pour Q1, Q4, Q6, Q8, Q9 (Recherche par nom et filtres par catégorie)
    "CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(name);",
    "CREATE INDEX IF NOT EXISTS idx_principals_cat ON principals(category);",
    # Pour Q2, Q6, Q8, Q9 (Filtres par année)
    "CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(year);",
    # Pour Q2, Q5, Q7 (Recherche par genre)
    "CREATE INDEX IF NOT EXISTS idx_genres_genre ON genres(genre);",
    # Pour Q2, Q5, Q7, Q8 (Tris et filtres sur les notes/votes)
    "CREATE INDEX IF NOT EXISTS idx_ratings_perf ON ratings(average_rating, num_votes);",
    # Pour les Jointures (FK) entre tables
    "CREATE INDEX IF NOT EXISTS idx_principals_movie ON principals(movie_id);",
    "CREATE INDEX IF NOT EXISTS idx_principals_person ON principals(person_id);",
    "CREATE INDEX IF NOT EXISTS idx_characters_movie ON characters(movie_id);",
    "CREATE INDEX IF NOT EXISTS idx_characters_person ON characters(person_id);",
]


def get_db_size():
    """Retourne la taille du fichier DB en Mo"""
    if os.path.exists(DB_PATH):
        return os.path.getsize(DB_PATH) / (1024 * 1024)
    return 0


def drop_indexes(conn):
    """Supprime tous les index créés pour repartir à zéro"""
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF;")
    for idx_sql in INDEXES:
        # On extrait le nom de l'index de la requête SQL
        idx_name = idx_sql.split("INDEX IF NOT EXISTS ")[1].split(" ON")[0]
        cursor.execute(f"DROP INDEX IF EXISTS {idx_name};")
    conn.commit()
    print("  Index supprimés (État initial)")


def create_indexes(conn):
    """Crée les index optimisés"""
    cursor = conn.cursor()
    print("  Création des index en cours...", end=" ", flush=True)
    start = time.time()
    for sql in INDEXES:
        cursor.execute(sql)
    conn.commit()
    print(f"Fait en {time.time() - start:.2f}s")


def run_benchmark():
    if not os.path.exists(DB_PATH):
        print(f"❌ Base introuvable : {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # --- DÉFINITION DES TÂCHES DE REQUÊTES ---
    tasks = [
        (
            "Q1 - Filmographie",
            lambda: queries.query_actor_filmography(conn, "Brad Pitt"),
        ),
        (
            "Q2 - Top N films",
            lambda: queries.query_top_movies_by_genre(conn, "Action", 2000, 2010, 10),
        ),
        ("Q3 - Multi-rôles", lambda: queries.query_multi_role_actors(conn)),
        (
            "Q4 - Collaborations",
            lambda: queries.query_director_collaborations(conn, "Johnny Depp"),
        ),
        ("Q5 - Genres Pop.", lambda: queries.query_popular_genres(conn)),
        ("Q6 - Carrière", lambda: queries.query_actor_career_stats(conn, "Tom Hanks")),
        ("Q7 - Classement Genre", lambda: queries.query_top_3_per_genre(conn)),
        ("Q8 - Percée", lambda: queries.query_breakthrough_actors(conn)),
        ("Q9 - Longévité", lambda: queries.query_longest_careers(conn)),
    ]

    results = {}

    # --- PHASE 1 : SANS INDEX ---
    print("\n--- ⏱️  MESURE SANS INDEX ---")
    drop_indexes(conn)
    size_before = get_db_size()

    for name, func in tasks:
        start = time.time()
        # On exécute
        func()
        duration_ms = (time.time() - start) * 1000
        results[name] = {"before": duration_ms}
        print(f"   👉 {name:<25} : {duration_ms:.2f} ms")

    # --- PHASE 2 : AVEC INDEX ---
    print("\n--- ⏱️  MESURE AVEC INDEX ---")
    create_indexes(conn)
    size_after = get_db_size()

    for name, func in tasks:
        start = time.time()
        func()
        duration_ms = (time.time() - start) * 1000
        results[name]["after"] = duration_ms
        print(f"   👉 {name:<25} : {duration_ms:.2f} ms")

    # --- AFFICHAGE DU RAPPORT FINAL ---
    print("\n" + "=" * 85)
    print(
        f"{'REQUÊTE':<25} | {'SANS INDEX (ms)':<15} | {'AVEC INDEX (ms)':<15} | {'GAIN (%)':<10}"
    )
    print("-" * 85)

    for name, data in results.items():
        before = data["before"]
        after = data["after"]
        # Calcul du gain : (Avant - Après) / Avant * 100
        gain = ((before - after) / before) * 100 if before > 0 else 0

        gain_str = f"{gain:+.1f}%"

        print(f"{name:<25} | {before:15.2f} | {after:15.2f} | {gain_str:10}")

    print("=" * 85)
    print(f"\n📦 IMPACT STOCKAGE :")
    print(f"   - Taille avant index : {size_before:.2f} Mo")
    print(f"   - Taille après index : {size_after:.2f} Mo")
    print(f"   - Surcoût index      : +{size_after - size_before:.2f} Mo")

    conn.close()


if __name__ == "__main__":
    run_benchmark()
