import pymongo
import time
import sys

# Connexion au Replica Set (liste des 3 noeuds)
URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
DB_NAME = "test_failover"


def monitor_replication():
    print("Connexion au cluster rs0 en cours...")

    client = pymongo.MongoClient(URI, serverSelectionTimeoutMS=2000)
    db = client[DB_NAME]
    coll = db["ping"]

    # Nettoyage de la collection de test precedente
    try:
        coll.drop()
    except Exception:
        pass

    counter = 0
    print("Demarrage du test d'ecriture continue.")
    print("Le script insere un document chaque seconde.")
    print("-" * 70)

    while True:
        try:
            # 1. Identification du noeud Primary actuel

            is_master = client.admin.command("ismaster")
            current_primary = is_master.get("primary", "INCONNU")

            # 2. Tentative d'ecriture
            doc = {"seq": counter, "timestamp": time.time(), "server": current_primary}
            coll.insert_one(doc)

            # 3. Verification du nombre total de documents (lecture)
            count = coll.count_documents({})

            print(
                f"[Seq {counter}] Ecriture reussie sur {current_primary} | Total documents: {count}"
            )

        except pymongo.errors.ServerSelectionTimeoutError:
            print(
                f"[Seq {counter}] ERREUR : Timeout de connexion (Aucun serveur disponible)"
            )
        except pymongo.errors.AutoReconnect:
            print(
                f"[Seq {counter}] ATTENTION : Reconnexion automatique en cours (Election probable)..."
            )
        except Exception as e:
            print(f"[Seq {counter}] ERREUR INATTENDUE : {e}")

        counter += 1
        time.sleep(1)


if __name__ == "__main__":
    try:
        monitor_replication()
    except KeyboardInterrupt:
        print("\nArret du test par l'utilisateur.")
