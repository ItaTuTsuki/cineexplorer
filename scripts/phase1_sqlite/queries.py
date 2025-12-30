import sqlite3
import os

# Configuration du chemin vers la DB
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "imdb.db")


def get_db_connection():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"La base {DB_PATH} est introuvable.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# REQUÊTE 1 : Filmographie d'un acteur
# =============================================================================
def query_actor_filmography(conn, actor_name: str) -> list:
    """
    Q1: Dans quels films a joué un acteur donné ?
    Retourne : Titre, Année, Personnage
    """
    sql = """
    SELECT 
        m.title, 
        m.year, 
        c.character_name
    FROM movies m
    JOIN principals p ON m.movie_id = p.movie_id
    JOIN persons pe ON p.person_id = pe.person_id
    LEFT JOIN characters c ON (m.movie_id = c.movie_id AND pe.person_id = c.person_id)
    WHERE pe.name LIKE ? 
      AND (p.category = 'actor' OR p.category = 'actress')
    ORDER BY m.year DESC
    LIMIT 20;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


# =============================================================================
# REQUÊTE 2 : Top N films d'un genre
# =============================================================================
def query_top_movies_by_genre(
    conn, genre: str, year_start: int, year_end: int, n: int
) -> list:
    """
    Q2: Les N meilleurs films d'un genre selon la note moyenne.
    Retourne : Titre, Année, Note
    """
    sql = """
    SELECT 
        m.title, 
        m.year, 
        r.average_rating
    FROM movies m
    JOIN genres g ON m.movie_id = g.movie_id
    JOIN ratings r ON m.movie_id = r.movie_id
    WHERE g.genre = ? 
      AND m.year BETWEEN ? AND ?
    ORDER BY r.average_rating DESC, r.num_votes DESC
    LIMIT ?;
    """
    return conn.execute(sql, (genre, year_start, year_end, n)).fetchall()


# =============================================================================
# REQUÊTE 3 : Acteurs multi-rôles
# =============================================================================
def query_multi_role_actors(conn) -> list:
    """
    Q3: Acteurs ayant joué plusieurs personnages dans un même film.
    Retourne : Nom Acteur, Titre Film, Nombre de rôles
    """
    sql = """
    SELECT 
        pe.name, 
        m.title, 
        COUNT(c.character_name) as role_count
    FROM characters c
    JOIN persons pe ON c.person_id = pe.person_id
    JOIN movies m ON c.movie_id = m.movie_id
    GROUP BY pe.person_id, m.movie_id
    HAVING role_count > 1
    ORDER BY role_count DESC
    LIMIT 20;
    """
    return conn.execute(sql).fetchall()


# =============================================================================
# REQUÊTE 4 : Collaborations
# =============================================================================
def query_director_collaborations(conn, actor_name: str) -> list:
    """
    Q4: Réalisateurs ayant travaillé avec un acteur spécifique.
    Retourne : Nom Réalisateur, Nombre de films ensemble
    """
    sql = """
    SELECT
        pe_director.name as director_name,
        COUNT(DISTINCT m.movie_id) as movie_count
    FROM principals p_actor
    JOIN persons pe_actor ON p_actor.person_id = pe_actor.person_id
    JOIN movies m ON p_actor.movie_id = m.movie_id
    JOIN principals p_director ON m.movie_id = p_director.movie_id
    JOIN persons pe_director ON p_director.person_id = pe_director.person_id
    WHERE pe_actor.name LIKE ? 
      AND p_actor.category IN ('actor', 'actress')
      AND p_director.category = 'director'
    GROUP BY pe_director.person_id
    ORDER BY movie_count DESC
    LIMIT 10;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


# =============================================================================
# REQUÊTE 5 : Genres populaires
# =============================================================================
def query_popular_genres(conn) -> list:
    """
    Q5: Genres notés > 7.0 (avec +50 films).
    Retourne : Genre, Note Moyenne
    """
    sql = """
    SELECT 
        g.genre, 
        ROUND(AVG(r.average_rating), 2) as avg_rating
    FROM genres g
    JOIN ratings r ON g.movie_id = r.movie_id
    GROUP BY g.genre
    HAVING avg_rating > 7.0 AND COUNT(g.movie_id) > 50
    ORDER BY avg_rating DESC;
    """
    return conn.execute(sql).fetchall()


# =============================================================================
# REQUÊTE 6 : Évolution de carrière
# =============================================================================
def query_actor_career_stats(conn, actor_name: str) -> list:
    """
    Q6: Statut par décennie pour un acteur.
    Retourne : Décennie, Nombre de films, Note Moyenne
    """
    sql = """
    WITH ActorMovies AS (
        SELECT 
            m.year, 
            r.average_rating
        FROM movies m
        JOIN principals p ON m.movie_id = p.movie_id
        JOIN persons pe ON p.person_id = pe.person_id
        LEFT JOIN ratings r ON m.movie_id = r.movie_id
        WHERE pe.name LIKE ? 
          AND m.year IS NOT NULL
    )
    SELECT 
        (year / 10) * 10 as decade,
        COUNT(*) as num_movies,
        ROUND(AVG(average_rating), 2) as avg_rating
    FROM ActorMovies
    GROUP BY decade
    ORDER BY decade;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


# =============================================================================
# REQUÊTE 7 : Classement par genre
# =============================================================================
def query_top_3_per_genre(conn) -> list:
    """
    Q7: Pour chaque genre, les 3 meilleurs films avec leur rang.
    Utilise RANK() OVER (PARTITION BY ...)
    Retourne : Genre, Rang, Titre, Note
    """
    sql = """
    WITH RankedMovies AS (
        SELECT 
            g.genre,
            m.title,
            r.average_rating,
            -- RANK() attribue un rang, réinitialisé à chaque changement de genre
            RANK() OVER (
                PARTITION BY g.genre 
                ORDER BY r.average_rating DESC, r.num_votes DESC
            ) as rank
        FROM genres g
        JOIN movies m ON g.movie_id = m.movie_id
        JOIN ratings r ON m.movie_id = r.movie_id
        WHERE r.num_votes > 1000 
    )
    SELECT genre, rank, title, average_rating
    FROM RankedMovies
    WHERE rank <= 3
    ORDER BY genre ASC, rank ASC;
    """
    return conn.execute(sql).fetchall()


# =============================================================================
# REQUÊTE 8 : Carrière propulsée
# =============================================================================
def query_breakthrough_actors(conn) -> list:
    """
    Q8: Personnes ayant percé grâce à un film (Premier succès >200k votes après des films <200k).
    Utilise ROW_NUMBER() pour ne garder que le 1er succès par acteur.
    """
    sql = """
    WITH Candidates AS (
        SELECT 
            pe.name,
            m_break.title as breakthrough_movie,
            m_break.year as breakthrough_year,
            -- On numérote les succès par ordre chronologique pour chaque acteur
            ROW_NUMBER() OVER (
                PARTITION BY pe.person_id 
                ORDER BY m_break.year ASC
            ) as rn
        FROM principals p
        JOIN persons pe ON p.person_id = pe.person_id
        -- Film succès (>200k votes)
        JOIN movies m_break ON p.movie_id = m_break.movie_id
        JOIN ratings r_break ON m_break.movie_id = r_break.movie_id
        -- Films précédents (<200k votes)
        JOIN principals p_before ON pe.person_id = p_before.person_id
        JOIN movies m_before ON p_before.movie_id = m_before.movie_id
        JOIN ratings r_before ON m_before.movie_id = r_before.movie_id
        
        WHERE r_break.num_votes > 200000
          AND r_before.num_votes < 200000
          AND m_before.year < m_break.year
          AND p.category IN ('actor', 'actress')
          
        GROUP BY pe.person_id, m_break.movie_id
        -- Il faut avoir fait au moins 3 petits films avant
        HAVING COUNT(DISTINCT m_before.movie_id) >= 3
    )
    SELECT name, breakthrough_movie, breakthrough_year
    FROM Candidates
    WHERE rn = 1 -- On ne garde que le tout premier succès
    LIMIT 10;
    """
    return conn.execute(sql).fetchall()


# =============================================================================
# REQUÊTE 9 : Requête libre (Longévité)
# =============================================================================
def query_longest_careers(conn) -> list:
    """
    Q9: Acteurs avec la plus grande longévité.
    Retourne : Nom, Durée de carrière (ans)
    """
    sql = """
    SELECT 
        pe.name,
        (MAX(m.year) - MIN(m.year)) as career_span
    FROM persons pe
    JOIN principals p ON pe.person_id = p.person_id
    JOIN movies m ON p.movie_id = m.movie_id
    WHERE p.category IN ('actor', 'actress')
      AND m.year IS NOT NULL
    GROUP BY pe.person_id
    HAVING career_span > 0 AND COUNT(m.movie_id) > 10
    ORDER BY career_span DESC
    LIMIT 15;
    """
    return conn.execute(sql).fetchall()


if __name__ == "__main__":
    try:
        conn = get_db_connection()

        print("--- Q1: Filmographie (Harrison Ford) ---")
        for row in query_actor_filmography(conn, "Harrison Ford"):
            print(f"{row['title']} ({row['year']}) - Rôle: {row['character_name']}")

        print("\n--- Q2: Top 5 Horror (1980-1990) ---")
        for row in query_top_movies_by_genre(conn, "Horror", 1980, 1990, 5):
            print(f"{row['title']} ({row['year']}) - Note: {row['average_rating']}")

        print("\n--- Q3: Acteurs Multi-Rôles ---")
        for row in query_multi_role_actors(conn):
            print(f"{row['name']} dans '{row['title']}' : {row['role_count']} rôles")

        print("\n--- Q4: Collaborations (Johnny Depp) ---")
        for row in query_director_collaborations(conn, "Johnny Depp"):
            print(f"{row['director_name']} : {row['movie_count']} films")

        print("\n--- Q5: Genres Populaires (>7.0) ---")
        for row in query_popular_genres(conn):
            print(f"{row['genre']} : {row['avg_rating']}/10")

        print("\n--- Q6: Carrière (Tom Hanks) ---")
        for row in query_actor_career_stats(conn, "Tom Hanks"):
            print(
                f"Années {row['decade']} : {row['num_movies']} films (Moyenne: {row['avg_rating']})"
            )

        print("\n--- Q7: Top 3 par Genre ---")
        for row in query_top_3_per_genre(conn):
            print(
                f"{row['genre']} #{row['rank']} : {row['title']} (Note: {row['average_rating']})"
            )

        print("\n--- Q8: Carrières Propulsées ---")
        for row in query_breakthrough_actors(conn):
            print(
                f"{row['name']} grâce à '{row['breakthrough_movie']}' ({row['breakthrough_year']}) "
            )

        print("\n--- Q9: Longévité ---")
        for row in query_longest_careers(conn):
            print(f"{row['name']} : {row['career_span']} ans de carrière")

        conn.close()
    except Exception as e:
        print(f" Erreur : {e}")
