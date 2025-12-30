import os
import pandas as pd

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, "data", "csv")


def inspect_data():
    if not os.path.exists(CSV_DIR):
        print(f"❌ Dossier introuvable : {CSV_DIR}")
        return

    files = [f for f in os.listdir(CSV_DIR) if f.endswith(".csv")]

    print(f"🔍 INSPECTION DU CONTENU (2 premières lignes) : {CSV_DIR}\n")

    for filename in files:
        filepath = os.path.join(CSV_DIR, filename)
        try:
            print("=" * 60)
            print(f"📁 FICHIER : {filename}")
            print("=" * 60)

            # On lit les 2 premières lignes de données
            # low_memory=False évite les warnings sur les types mixtes
            df = pd.read_csv(filepath, nrows=2)

            # 1. Afficher les colonnes brutes (pour voir le format ('mid',) etc.)
            print(f"📌 En-têtes bruts ({len(df.columns)}) :")
            print(list(df.columns))

            # 2. Afficher un aperçu propre des données
            print("\n📌 Aperçu des données :")
            # On force l'affichage de toutes les colonnes
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", 1000)
            print(df.head(2))
            print("\n\n")

        except Exception as e:
            print(f"❌ Erreur lecture {filename}: {e}\n")


if __name__ == "__main__":
    inspect_data()
