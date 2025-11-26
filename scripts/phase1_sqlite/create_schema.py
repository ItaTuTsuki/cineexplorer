import sqlite3
import os

# Chemin vers la base de données
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'imdb.db')

def create_schema():
    # Si la base existe déjà, on la supprime pour repartir propre avec le nouveau schéma étendu
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"🗑️ Ancienne base supprimée : {DB_PATH}")

    print(f"Création de la base de données dans : {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    # --- 1. Tables Principales ---

    # Table MOVIES (Étendue avec original_title, is_adult, etc.)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        movie_id TEXT PRIMARY KEY,
        title_type TEXT,
        title TEXT,
        original_title TEXT,
        is_adult INTEGER,
        year INTEGER,
        end_year INTEGER,
        runtime INTEGER
    );
    """)

    # Table PERSONS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS persons (
        person_id TEXT PRIMARY KEY,
        name TEXT,
        birth_year INTEGER,
        death_year INTEGER
    );
    """)

    # --- 2. Tables de Détails ---

    # Table RATINGS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
        movie_id TEXT PRIMARY KEY,
        average_rating REAL,
        num_votes INTEGER,
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    );
    """)

    # Table TITLES (Étendue avec ordering, types, attributes...)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS titles (
        title_id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id TEXT,
        ordering INTEGER,
        title TEXT,
        region TEXT,
        language TEXT,
        types TEXT,
        attributes TEXT,
        is_original_title INTEGER,
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    );
    """)

    # Table GENRES
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS genres (
        movie_id TEXT,
        genre TEXT,
        PRIMARY KEY (movie_id, genre),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    );
    """)

    # --- 3. Tables de Relations (Casting & Crew) ---

    # Table PRINCIPALS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS principals (
        movie_id TEXT,
        ordering INTEGER,
        person_id TEXT,
        category TEXT,
        job TEXT,
        PRIMARY KEY (movie_id, person_id, category, ordering),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    );
    """)

    # Table CHARACTERS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS characters (
        movie_id TEXT,
        person_id TEXT,
        character_name TEXT,
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    );
    """)

    # Table DIRECTORS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS directors (
        movie_id TEXT,
        person_id TEXT,
        PRIMARY KEY (movie_id, person_id),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    );
    """)

    # Table WRITERS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS writers (
        movie_id TEXT,
        person_id TEXT,
        PRIMARY KEY (movie_id, person_id),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    );
    """)

    # --- 4. Nouvelles Tables découvertes ---

    # Table PROFESSIONS (jobs des personnes)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS professions (
        person_id TEXT,
        job_name TEXT,
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    );
    """)

    # Table KNOWN_FOR_MOVIES (films pour lesquels une personne est connue)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS known_for_movies (
        person_id TEXT,
        movie_id TEXT,
        FOREIGN KEY (person_id) REFERENCES persons(person_id),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    );
    """)

    conn.commit()
    conn.close()
    print("✅ Nouveau schéma COMPLET créé avec succès !")

if __name__ == "__main__":
    create_schema()