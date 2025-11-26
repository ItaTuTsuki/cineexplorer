import sqlite3
import os

# Chemin vers la base de données (à la racine du dossier data)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'imdb.db')

def create_schema():
    print(f"Création de la base de données dans : {DB_PATH}")
    
    # Connexion (crée le fichier s'il n'existe pas)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Activation des clés étrangères (important pour SQLite)
    cursor.execute("PRAGMA foreign_keys = ON;")

    # --- 1. Tables Principales ---

    # Table MOVIES
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        movie_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        year INTEGER,
        runtime INTEGER
    );
    """)

    # Table PERSONS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS persons (
        person_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        birth_year INTEGER,
        death_year INTEGER
    );
    """)

    # --- 2. Tables de Détails (Relations 1-1 ou 1-N simples) ---

    # Table RATINGS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
        movie_id TEXT PRIMARY KEY,
        average_rating REAL,
        num_votes INTEGER,
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    );
    """)

    # Table TITLES (Titres alternatifs)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS titles (
        title_id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id TEXT,
        title TEXT,
        region TEXT,
        language TEXT,
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

    # --- 3. Tables de Relations (Many-to-Many) ---

    # Table PRINCIPALS (Casting principal)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS principals (
        movie_id TEXT,
        person_id TEXT,
        category TEXT,
        job TEXT,
        ordering INTEGER,
        PRIMARY KEY (movie_id, person_id, category),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    );
    """)

    # Table CHARACTERS (Rôles joués)
    # Note : Souvent lié à principals, mais les CSV séparent parfois ces données
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

    conn.commit()
    conn.close()
    print("✅ Schéma créé avec succès !")

if __name__ == "__main__":
    # S'assurer que le dossier data existe
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    create_schema()