import sqlite3
import os

# Chemin vers votre base de données
# Attention : assurez-vous d'être dans le bon dossier quand vous lancez le script
DB_PATH = 'data/imdb.db'

def clean_orphans(conn, table_name, fk_col, parent_table, parent_pk):
    """Supprime les lignes qui pointent vers un parent inexistant."""
    print(f"🔍 Vérification '{table_name}' ({fk_col} -> {parent_table}.{parent_pk})...")
    
    cursor = conn.cursor()
    
    # Compter les orphelins
    query_count = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE {fk_col} NOT IN (SELECT {parent_pk} FROM {parent_table})
    """
    cursor.execute(query_count)
    count = cursor.fetchone()[0]
    
    if count > 0:
        print(f"⚠️  Trouvé {count} orphelins ! Nettoyage en cours...")
        # Suppression
        query_delete = f"""
            DELETE FROM {table_name}
            WHERE {fk_col} NOT IN (SELECT {parent_pk} FROM {parent_table})
        """
        cursor.execute(query_delete)
        conn.commit()
        print(f"✅ Nettoyé.")
    else:
        print(f"✅ OK.")

def fix_database():
    if not os.path.exists(DB_PATH):
        print(f"❌ Erreur : Base introuvable à {DB_PATH}")
        return

    print(f"🔌 Connexion à {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # 1. Les tables qui lient des Personnes
        clean_orphans(conn, "writers", "person_id", "persons", "person_id")
        clean_orphans(conn, "directors", "person_id", "persons", "person_id")
        clean_orphans(conn, "principals", "person_id", "persons", "person_id")
        
        # --- AJOUT ICI ---
        clean_orphans(conn, "characters", "person_id", "persons", "person_id")
        
        # 2. Les tables qui lient des Films
        clean_orphans(conn, "ratings", "movie_id", "movies", "movie_id")
        clean_orphans(conn, "characters", "movie_id", "movies", "movie_id")
        clean_orphans(conn, "titles", "movie_id", "movies", "movie_id")
        clean_orphans(conn, "genres", "movie_id", "movies", "movie_id")

        print("\n🎉 Base de données entièrement vérifiée !")
        
    except Exception as e:
        print(f"\n❌ Une erreur est survenue : {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    fix_database()