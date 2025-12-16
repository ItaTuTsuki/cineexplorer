import pymongo
import sys

def test_mongo_connection():
    # L'adresse standard locale
    uri = "mongodb://localhost:27017/"
    
    print(f" Tentative de connexion à : {uri}")
    print("..." )

    try:
        # 1. Création du client (avec un timeout court de 5s pour pas attendre 30s si ça plante)
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # 2. Le test réel : on demande les infos du serveur
        # (C'est ici que ça plante si le serveur est éteint)
        info = client.server_info()
        
        print(" SUCCÈS ! Connexion établie.")
        print(f" Version MongoDB : {info.get('version')}")
        print(f" Système : {info.get('sysInfo')}")
        
        # 3. Lister les bases existantes
        dbs = client.list_database_names()
        print(f" Bases de données existantes : {dbs}")
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print("\n ERREUR : Le serveur ne répond pas.")
        print("Avez-vous bien lancé la commande 'mongod' dans un autre terminal ?")
    except Exception as e:
        print(f"\n ERREUR inattendue : {e}")

if __name__ == "__main__":
    test_mongo_connection()