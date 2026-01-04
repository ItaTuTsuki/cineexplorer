from django.http import HttpResponse
from services.mongo_service import get_movies_collection

def test_connection(request):
    try:
        # On récupère la collection via notre service
        collection = get_movies_collection()
        
        # 1. On compte simplement les documents
        nb_films = collection.count_documents({})
        
        # 2. On récupère l'adresse du serveur connecté (le Primary)
        # client.address renvoie un tuple comme ('localhost', 27017)
        server_info = collection.database.client.address
        
        # 3. On renvoie une page HTML toute simple
        html = f"""
        <h1>Test Connexion MongoDB</h1>
        <hr>
        <p>Si vous voyez ça, c'est que Django communique avec MongoDB</p>
        <ul>
            <li><strong>Base de données :</strong> imdb_project</li>
            <li><strong>Collection :</strong> movies_complete</li>
            <li><strong>Nombre de films trouvés :</strong> {nb_films}</li>
            <li><strong>Serveur connecté (Primary) :</strong> {server_info}</li>
        </ul>
        """
        return HttpResponse(html)

    except Exception as e:
        # En cas d'erreur, on l'affiche simplement sur la page
        return HttpResponse(f"<h1>Erreur de connexion</h1><p>{e}</p>")