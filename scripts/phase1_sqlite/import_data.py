import sqlite3
import pandas as pd
import os
import time
import re

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, 'data', 'imdb.db')
CSV_DIR = os.path.join(BASE_DIR, 'data', 'csv')

# CORRECTION ICI : On ne mappe que primaryTitle vers title pour éviter les doublons
COLUMN_MAPPING = {
    # Films
    'mid': 'movie_id',
    'primaryTitle': 'title',  # On prend le titre principal
    # 'originalTitle': 'title', <-- LIGNE SUPPRIMÉE (cause de l'erreur)
    'startYear': 'year',
    'runtimeMinutes': 'runtime',
    
    # Personnes
    'pid': 'person_id',
    'primaryName': 'name',
    'birthYear': 'birth_year',
    'deathYear': 'death_year',
    
    # Ratings
    'averageRating': 'average_rating',
    'numVotes': 'num_votes',
    
    # Titles
    'title': 'title',
    'region': 'region',
    'language': 'language',
    
    # Genres
    'genre': 'genre',
    
    # Principals
    'category': 'category',
    'job': 'job',
    'ordering': 'ordering',
    
    # Characters
    'character': 'character_name',
    'characters': 'character_name'
}

TABLES_CONFIG = [
    ('movies.csv', 'movies', ['movie_id', 'title', 'year', 'runtime']),
    ('persons.csv', 'persons', ['person_id', 'name', 'birth_year', 'death_year']),
    ('ratings.csv', 'ratings', ['movie_id', 'average_rating', 'num_votes']),
    ('titles.csv', 'titles', ['movie_id', 'title', 'region', 'language']),
    ('genres.csv', 'genres', ['movie_id', 'genre']),
    ('principals.csv', 'principals', ['movie_id', 'person_id', 'category', 'job', 'ordering']),
    ('directors.csv', 'directors', ['movie_id', 'person_id']),
    ('writers.csv', 'writers', ['movie_id', 'person_id']),
    ('characters.csv', 'characters', ['movie_id', 'person_id', 'character_name']),
]

def clean_header(col_name):
    clean = re.sub(r"[()',]", "", str(col_name))
    return clean.strip()

def import_data():
    if not os.path.exists(DB_PATH):
        print(f"❌ Erreur : Base {DB_PATH} introuvable.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = MEMORY")
    cursor.execute("PRAGMA foreign_keys = OFF")

    start_global = time.time()

    for csv_file, table_name, sql_columns in TABLES_CONFIG:
        csv_path = os.path.join(CSV_DIR, csv_file)
        
        if not os.path.exists(csv_path):
            print(f"⚠️  Fichier introuvable : {csv_file}")
            continue

        print(f"\nTraitement de {table_name.upper()} (depuis {csv_file})...")
        
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            
            # Nettoyage des headers
            df.columns = [clean_header(c) for c in df.columns]
            
            # Renommage
            df = df.rename(columns=COLUMN_MAPPING)
            
            # Logique de secours pour le titre (si primaryTitle manque mais originalTitle est là)
            if table_name == 'movies' and 'title' not in df.columns and 'originalTitle' in df.columns:
                 df['title'] = df['originalTitle']

            # Vérification des colonnes manquantes
            missing_cols = [col for col in sql_columns if col not in df.columns]

            if missing_cols:
                # Si c'est des colonnes non-critiques, on remplit avec None
                critical_cols = ['movie_id', 'person_id']
                if any(c in missing_cols for c in critical_cols):
                    print(f"❌ Erreur critique : Colonnes manquantes {missing_cols}")
                    continue
                else:
                    for col in missing_cols:
                        df[col] = None

            # Sélection finale stricte pour éviter les doublons
            df_final = df[sql_columns]
            
            # Nettoyage NaN
            df_final = df_final.where(pd.notnull(df_final), None)
            
            # Insertion
            data_to_insert = list(df_final.itertuples(index=False, name=None))
            
            # Vérification de sécurité avant insertion
            if len(data_to_insert) > 0 and len(data_to_insert[0]) != len(sql_columns):
                print(f"❌ Erreur dimension : SQL attend {len(sql_columns)} cols, données ont {len(data_to_insert[0])}")
                continue

            placeholders = ', '.join(['?'] * len(sql_columns))
            sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(sql_columns)}) VALUES ({placeholders})"
            
            cursor.executemany(sql, data_to_insert)
            conn.commit()
            print(f"✅ {len(data_to_insert)} lignes insérées.")
            
        except Exception as e:
            print(f"❌ Erreur sur {table_name}: {e}")

    cursor.execute("PRAGMA foreign_keys = ON")
    conn.close()
    print(f"\n🎉 IMPORT TERMINÉ en {time.time() - start_global:.2f} s.")

if __name__ == "__main__":
    import_data()