import os
import sys

# MODIFICATION ICI : On utilise le dossier courant "."
ROOT_DIR = "."

# Structure des dossiers à créer
DIRECTORIES = [
    "config",
    "data/csv",
    "data/mongo/standalone/db-1",
    "data/mongo/standalone/db-2",
    "data/mongo/standalone/db-3",
    "scripts/phase1_sqlite",
    "scripts/phase2_mongodb",
    "scripts/phase3_replica",
    "movies",
    "services",
    "templates/movies",
    "static/css",
    "static/js",
    "static/img",
    "reports/livrable1",
    "reports/livrable2",
    "reports/livrable3",
    "reports/final",
]

# Fichiers à créer
FILES = {
    "requirements.txt": "Django>=4.0\npymongo\npandas\nmatplotlib\njupyter\nnotebook\n",
    "README.md": "# Projet CinéExplorer\n\nProjet de base de données avancées (SQL & NoSQL avec Django).",
    ".gitignore": "*.pyc\n__pycache__\n/venv\n/data/csv/\n*.db\n/data/mongo/\n.DS_Store\n",
    # Config Django
    "config/__init__.py": "",
    "config/settings.py": "# Configuration Django\n",
    "config/urls.py": "# URLs du projet\n",
    "config/wsgi.py": "# WSGI config\n",
    # Data
    "data/exploration.ipynb": '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}',
    # Phase 1
    "scripts/phase1_sqlite/__init__.py": "",
    "scripts/phase1_sqlite/create_schema.py": "# T1.1: Conception du schéma relationnel\n",
    "scripts/phase1_sqlite/import_data.py": "# T1.2: Import des données\n",
    "scripts/phase1_sqlite/queries.py": "# T1.3: Requêtes SQL\n",
    "scripts/phase1_sqlite/benchmark.py": "# T1.4: Indexation et benchmark\n",
    # Phase 2
    "scripts/phase2_mongodb/__init__.py": "",
    "scripts/phase2_mongodb/migrate_flat.py": "# T2.2: Migration des collections plates\n",
    "scripts/phase2_mongodb/migrate_structured.py": "# T2.4: Documents structurés\n",
    "scripts/phase2_mongodb/queries_mongo.py": "# T2.3: Requêtes MongoDB équivalentes\n",
    "scripts/phase2_mongodb/compare_performance.py": "# Comparaison SQL vs NoSQL\n",
    # Phase 3
    "scripts/phase3_replica/__init__.py": "",
    "scripts/phase3_replica/setup_replica.sh": "# T3.1: Configuration du Replica Set\n",
    "scripts/phase3_replica/test_failover.py": "# T3.2: Tests de tolérance aux pannes\n",
    # App Django
    "movies/__init__.py": "",
    "movies/models.py": "# Modèles Django (Phase 4)\n",
    "movies/views.py": "# Vues Django\n",
    "movies/urls.py": "# URLs de l'application movies\n",
    # Services
    "services/__init__.py": "",
    "services/sqlite_service.py": "# Fonctions d'accès SQLite\n",
    "services/mongo_service.py": "# Fonctions d'accès MongoDB\n",
    # Templates
    "templates/movies/base.html": "\n",
    "templates/movies/home.html": "\n",
    "templates/movies/list.html": "\n",
    "templates/movies/detail.html": "\n",
    "templates/movies/search.html": "\n",
    "templates/movies/stats.html": "\n",
}


def create_structure():
    # On travaille dans le dossier courant
    root_path = os.getcwd()

    print(f"📂 Installation dans : {root_path}")

    # Création des sous-dossiers
    for folder in DIRECTORIES:
        path = os.path.join(root_path, folder)
        os.makedirs(path, exist_ok=True)

    # Création des fichiers
    for filename, content in FILES.items():
        file_path = os.path.join(root_path, filename)

        if not os.path.exists(file_path):
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"📄 Fichier créé : {filename}")
        else:
            print(f"⏩ Ignoré (existe déjà) : {filename}")

    print("\n" + "=" * 50)
    print("✅ PROJET INITIALISÉ AVEC SUCCÈS")
    print("=" * 50)


if __name__ == "__main__":
    create_structure()
