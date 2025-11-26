import sqlite3
import pandas as pd
import os
import time
import re

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, 'data', 'imdb.db')
CSV_DIR = os.path.join(BASE_DIR, 'data', 'csv')

# MAPPING COMPLET (CSV brut nettoyé -> Colonne SQL)
COLUMN_MAPPING = {
    # Clés communes
    'mid': 'movie_id',
    'pid': 'person_id',

    # Movies
    'titleType': 'title_type',
    'primaryTitle': 'title',
    'originalTitle': 'original_title',
    'isAdult': 'is_adult',
    'startYear': 'year',
    'endYear': 'end_year',
    'runtimeMinutes': 'runtime',
    
    # Persons
    'primaryName': 'name',
    'birthYear': 'birth_year',
    'deathYear': 'death_year',
    
    # Ratings
    'averageRating': 'average_rating',
    'numVotes': 'num_votes',
    
    # Titles
    'ordering': 'ordering',
    'title': 'title',
    'region': 'region',
    'language': 'language',
    'types': 'types',
    'attributes': 'attributes',
    'isOriginalTitle': 'is_original_title',
    
    # Genres & Professions
    'genre': 'genre',
    'jobName': 'job_name',
    
    # Principals
    'category': 'category',
    'job': 'job',
    
    # Characters
    'name': 'character_name', # Dans characters.csv, 'name' est le nom du perso
}

# CONFIGURATION DES TABLES (Fichier CSV, Table SQL, Liste des colonnes à remplir)
TABLES_CONFIG = [
    # 1. Parents
    ('movies.csv', 'movies', 
     ['movie_id', 'title_type', 'title', 'original_title', 'is_adult', 'year', 'end_year', 'runtime']),
    
    ('persons.csv', 'persons', 
     ['person_id', 'name', 'birth_year', 'death_year']),
    
    # 2. Enfants directs
    ('ratings.csv', 'ratings', 
     ['movie_id', 'average_rating', 'num_votes']),
    
    ('titles.csv', 'titles', 
     ['movie_id', 'ordering', 'title', 'region', 'language', 'types', 'attributes', 'is_original_title']),
    
    ('genres.csv', 'genres', 
     ['movie_id', 'genre']),

    ('professions.csv', 'professions', 
     ['person_id', 'job_name']),

    # 3. Relations
    ('principals.csv', 'principals', 
     ['movie_id', 'ordering', 'person_id', 'category', 'job']),
    
    ('characters.csv', 'characters', 
     ['movie_id', 'person_id', 'character_name']),
    
    ('directors.csv', 'directors', 
     ['movie_id', 'person_id']),
    
    ('writers.csv', 'writers', 
     ['movie_id', 'person_id']),
     
    ('knownformovies.csv', 'known_for_movies', 
     ['person_id', 'movie_id']),
]

def clean_header(col_name):
    """Nettoie les en-têtes: ('mid',) -> mid"""
    clean = re.sub(r"[()',]", "", str(col_name))
    return clean.strip()

def import_data():
    if not os.path.exists(DB_PATH):
        print(f"❌ Erreur : Base {DB_PATH} introuvable.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Accélérateur
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = MEMORY")
    cursor.execute("PRAGMA foreign_keys = OFF")

    start_global = time.time()

    for csv_file, table_name, sql_columns in TABLES_CONFIG:
        csv_path = os.path.join(CSV_DIR, csv_file)
        
        if not os.path.exists(csv_path):
            print(f"⏩ Fichier manquant : {csv_file} (Table {table_name} ignorée)")
            continue

        print(f"\nTraitement de {table_name.upper()} (depuis {csv_file})...")
        
        try:
            # 1. Lecture
            df = pd.read_csv(csv_path, low_memory=False)
            
            # 2. Nettoyage headers
            df.columns = [clean_header(c) for c in df.columns]
            
            # 3. Renommage vers SQL
            df = df.rename(columns=COLUMN_MAPPING)
            
            # 4. Vérification et Remplissage
            missing_cols = [col for col in sql_columns if col not in df.columns]
            if missing_cols:
                # Si des colonnes manquent, on remplit avec None
                for col in missing_cols:
                    df[col] = None

            # 5. Sélection et Nettoyage Données
            df_final = df[sql_columns]
            df_final = df_final.where(pd.notnull(df_final), None)
            
            # 6. Insertion
            data_to_insert = list(df_final.itertuples(index=False, name=None))
            
            placeholders = ', '.join(['?'] * len(sql_columns))
            sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(sql_columns)}) VALUES ({placeholders})"
            
            cursor.executemany(sql, data_to_insert)
            conn.commit()
            print(f"✅ {len(data_to_insert)} lignes insérées.")
            
        except Exception as e:
            print(f"❌ Erreur sur {table_name}: {e}")

    cursor.execute("PRAGMA foreign_keys = ON")
    conn.close()
    print(f"\n🎉 IMPORT GLOBAL TERMINÉ en {time.time() - start_global:.2f} s.")

if __name__ == "__main__":
    import_data()