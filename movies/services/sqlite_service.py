from django.db import connection

def execute_raw_sql(query, params=None):
    """Exécute une requête SQL brute sur la base SQLite"""
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])
        # Si c'est un SELECT, on retourne les résultats
        if query.strip().upper().startswith("SELECT"):
            columns = [col[0] for col in cursor.description]
            return [
                dict(zip(columns, row)) 
                for row in cursor.fetchall()
            ]
        return None