import os
from pathlib import Path

# --- CONFIGURATION ---
# Dossiers à ignorer
IGNORE_FOLDERS = {
    "venv",
    ".git",
    ".idea",
    ".vscode",
    "__pycache__",
    "env",
    ".ipynb_checkpoints",
}
# Extensions de fichiers à ignorer
IGNORE_EXTENSIONS = {".pyc", ".pyo", ".pyd", ".DS_Store"}


def print_tree(dir_path, prefix=""):
    """Affiche l'arborescence de manière récursive"""
    path = Path(dir_path)

    # Récupérer tous les éléments du dossier
    try:
        # On filtre directement ici
        files_and_dirs = [
            x
            for x in path.iterdir()
            if x.name not in IGNORE_FOLDERS and x.suffix not in IGNORE_EXTENSIONS
        ]
    except PermissionError:
        return

    # Trier : Dossiers d'abord, puis fichiers, par ordre alphabétique
    files_and_dirs.sort(key=lambda x: (not x.is_dir(), x.name.lower()))

    # Nombre total d'éléments à afficher à ce niveau
    count = len(files_and_dirs)

    for i, item in enumerate(files_and_dirs):
        # Est-ce le dernier élément de la liste ?
        is_last = i == (count - 1)

        connector = "└── " if is_last else "├── "

        print(f"{prefix}{connector}{item.name}")

        if item.is_dir():
            # Préparer le préfixe pour le niveau suivant
            extension = "    " if is_last else "│   "
            print_tree(item, prefix + extension)


if __name__ == "__main__":
    current_dir = os.getcwd()
    print(f"\n📂 Structure du projet : {os.path.basename(current_dir)}")
    print("=" * 40)
    print_tree(current_dir)
    print("=" * 40)
