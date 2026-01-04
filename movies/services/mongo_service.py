from pymongo import MongoClient
from django.conf import settings

_client = None

def get_mongo_db():
    """Retourne l'objet Database MongoDB (Singleton)"""
    global _client
    if _client is None:
        # On utilise l'URI définie dans settings.py
        _client = MongoClient(
            settings.MONGO_URI, 
            serverSelectionTimeoutMS=5000
        )
    return _client[settings.MONGO_DB_NAME]

def get_movies_collection():
    """Raccourci vers la collection movies_complete"""
    db = get_mongo_db()
    return db['movies_complete']