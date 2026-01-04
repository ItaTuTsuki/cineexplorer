# CineExplorer

Plateforme web d'exploration cinématographique utilisant une architecture hybride (SQLite et MongoDB Replica Set).

Projet réalisé dans le cadre du module Base de Données Avancées (BDA).

## Structure du projet

```
cineexplorer/
├── config/                     # Configuration Django
├── data/
│   ├── csv/                    # EMPLACEMENT OBLIGATOIRE DES FICHIERS SOURCES IMDB
│   ├── imdb.db                 # Base SQLite générée
│   └── mongo/                  # Stockage des données MongoDB
├── movies/                     # Application Web Django
├── scripts/
│   ├── phase1_sqlite/
│   │   ├── create_schema.py    # Création des tables et contraintes
│   │   ├── import_data.py      # Importation des données CSV vers SQLite
│   │   ├── queries.py          # Requêtes SQL de test
│   │   └── benchmark.py        # Tests de performance SQL
│   ├── phase2_mongodb/
│   │   ├── migrate_flat.py     # Nettoyage SQLite + Migration MongoDB Flat
│   │   ├── migrate_structured.py # Transformation vers MongoDB Structured
│   │   ├── queries_mongo.py    # Requêtes sur modèle Flat
│   │   ├── queries_structured.py # Requêtes sur modèle Structured
│   │   └── compare_performance.py # Comparatif Flat vs Structured
│   └── phase3_replica/
│       └── test_failover.py    # Test de résistance aux pannes
├── manage.py
└── requirements.txt
```

## Prérequis

- Python 3.10 ou supérieur.
- MongoDB Community Server (installé et accessible via la commande mongod).
- Git.
- Espace disque : Environ 1 Go pour les données brutes et les bases.

## Installation

1. Cloner le dépôt
```
   git clone <url_du_repo>
   cd cineexplorer
```

2. Configurer l'environnement virtuel
   # Windows
   ```
   python -m venv venv
   .\venv\Scripts\activate
   ```

   # Mac/Linux
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Installer les dépendances
    ```
   pip install -r requirements.txt
    ```

4. Préparer les données sources
   Téléchargez les datasets IMDB (name.basics.tsv, title.basics.tsv, etc.), convertissez-les en CSV si nécessaire, et placez-les impérativement dans le dossier :
   ```
   cineexplorer/data/csv/
   ```

---

## GUIDE D'EXECUTION (Ordre Chronologique Impératif)

Suivez scrupuleusement ces étapes pour initialiser le projet.

### Phase 1 : Construction de la Base Relationnelle (SQLite)

Cette phase construit l'architecture SQL et importe les données brutes.

1. Création du Schéma
   Ce script crée les tables vides avec les types corrects et les contraintes.
   ```
   python scripts/phase1_sqlite/create_schema.py
   ```

2. Importation des Données
   Ce script lit les fichiers CSV du dossier data/csv/ et peuple la base de données SQLite.
   ```
   python scripts/phase1_sqlite/import_data.py
   ```

3. Vérification (Optionnel)
   Lancez les requêtes de test SQL.
   ```
   python scripts/phase1_sqlite/queries.py
   ```

4. Benchmark (Optionnel)
   Mesure les performances avec et sans index SQLite.
   ```
   python scripts/phase1_sqlite/benchmark.py
   ```

### Phase 2 : Migration et Structuration NoSQL (MongoDB)

Cette phase transforme le modèle relationnel en modèle orienté documents.
Prérequis : Une instance MongoDB doit être active sur le port 27017.

1. Test de connexion
   Vérifie que Python peut communiquer avec MongoDB.
   ```
   python scripts/phase2_mongodb/test_connection.py
   ```

2. Migration Initiale (Flat)
   Ce script nettoie d'abord la base SQLite (suppression des clés étrangères orphelines) pour garantir l'intégrité, puis migre les données vers des collections MongoDB simples.
   ```
   python scripts/phase2_mongodb/migrate_flat.py
   ```

3. Structuration des Données (Nested)
   Ce script utilise des pipelines d'agrégation pour transformer les collections plates en une collection unique movies_complete optimisée pour le web.
   ```
   python scripts/phase2_mongodb/migrate_structured.py
   ```

4. Benchmark Comparatif     
   Compare les performances entre le modèle Flat et le modèle Structured pour des requêtes typiques.
```
   python scripts/phase2_mongodb/compare_performance.py
```
### Phase 3 : Cluster Haute Disponibilité (Replica Set)

Transformation de l'instance unique en un cluster de 3 nœuds (Replica Set).

1. Création des dossiers de stockage
```
   mkdir data/mongo/db-1
   mkdir data/mongo/db-2
   mkdir data/mongo/db-3
```

2. Démarrage des Nœuds
   Ouvrez 3 terminaux distincts et lancez une commande par terminal :

   Terminal 1 :
   ```
   mongod --replSet rs0 --port 27017 --dbpath data/mongo/db-1 --bind_ip localhost
   ```

   Terminal 2 :
   ```
   mongod --replSet rs0 --port 27018 --dbpath data/mongo/db-2 --bind_ip localhost
   ```

   Terminal 3 :
   ```
   mongod --replSet rs0 --port 27019 --dbpath data/mongo/db-3 --bind_ip localhost
   ```

   Note : Si c'est la première fois que vous lancez le cluster, les bases seront vides. Il faudra relancer les scripts de la Phase 2 (migrate_flat.py puis migrate_structured.py) pour repeupler le cluster.

3. Initialisation du Cluster
   Dans un 4ème terminal :
   ```
   mongosh --port 27017
   ```

   Puis exécutez la configuration JavaScript suivante dans le shell :
   ```
   rs.initiate({
     _id: "rs0",
     members: [
       { _id: 0, host: "localhost:27017" },
       { _id: 1, host: "localhost:27018" },
       { _id: 2, host: "localhost:27019" }
     ]
   })
   ```

4. Test de Failover
   Ce script simule une charge client continue.
   ```
   python scripts/phase3_replica/test_failover.py
   ```

### Phase 4 : Interface Web (Django)

Lancement de l'application finale.
Prérequis : Le Replica Set (Phase 3) doit être en cours d'exécution.

1. Application des migrations Django
    ```
   python manage.py migrate
   ```

2. Lancement du serveur
   ```
   python manage.py runserver
   ```

3. Accès
   Ouvrez votre navigateur sur http://127.0.0.1:8000/

---

## Fonctionnalités de l'Application

* Accueil : Tableau de bord, Top 10, Suggestions aléatoires (Source : SQLite)
* Catalogue : Liste paginée, Filtres complexes Année/Genre/Note (Source : SQLite)
* Détail Film : Affichage complet Casting/Crew/Similaires (Source : MongoDB)
* Personne : Filmographie complète triée par métier (Source : SQLite)
* Recherche : Moteur global Films et Personnes (Source : SQLite)
* Statistiques : Graphiques interactifs Chart.js (Source : SQLite)

## Architecture Hybride

Le projet utilise le principe de persistance polyglotte :

* SQLite (Lecture/Recherche) : Utilisé pour les requêtes nécessitant des jointures complexes, des tris multicritères et des agrégations légères.
* MongoDB (Lecture Détail) : Utilisé pour récupérer en une seule opération l'intégralité des données d'un film via la collection structurée movies_complete.

