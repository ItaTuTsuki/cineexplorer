from django.shortcuts import render, Http404, get_object_or_404
from django.http import HttpResponse
from django.core.paginator import Paginator
from django.db.models import Count
from .models import Movies, Persons, Ratings, Genres, Principals, Directors, Writers
from movies.services.mongo_service import get_movies_collection
import random

def home(request):
    """Page d'accueil complète."""
    # 1. Stats
    total_movies = Movies.objects.count()
    total_persons = Persons.objects.count()
    total_directors = Directors.objects.values('person_id').distinct().count()
    
    # 2. Top 10 Films (Mieux notés)
    top_movies_qs = Ratings.objects.select_related('movie').order_by('-average_rating', '-num_votes')[:6]
    top_movies = _format_movies_for_template(top_movies_qs)

    # 3. Films Aléatoires (Suggestion)
    # On prend 50 films récents pour piocher dedans
    random_movies_qs = Movies.objects.filter(year__gte=2020).order_by('movie_id')[:50]
    # On en garde 6 au hasard
    random_selection = random.sample(list(random_movies_qs), min(len(random_movies_qs), 6))
    
    suggestions = []
    for m in random_selection:
        rating = Ratings.objects.filter(movie=m).first()
        suggestions.append({
            'movie_id': m.movie_id,
            'title': m.title,
            'year': m.year,
            'rating': rating.average_rating if rating else None,
            'votes': rating.num_votes if rating else 0
        })

    context = {
        'total_movies': total_movies,
        'total_actors': total_persons, 
        'total_directors': total_directors, 
        'top_movies': top_movies,
        'suggestions': suggestions
    }
    return render(request, 'movies/home.html', context)

def movie_list(request):
    """Catalogue avec filtres et pagination."""
    genre_filter = request.GET.get('genre')
    year_min = request.GET.get('year_min')
    year_max = request.GET.get('year_max')
    rating_min = request.GET.get('rating_min')
    sort_by = request.GET.get('sort', '-year')

    movies_qs = Movies.objects.select_related('ratings').all()

    if genre_filter:
        movies_qs = movies_qs.filter(genres__genre=genre_filter)
    if year_min:
        movies_qs = movies_qs.filter(year__gte=year_min)
    if year_max:
        movies_qs = movies_qs.filter(year__lte=year_max)
    if rating_min:
        movies_qs = movies_qs.filter(ratings__average_rating__gte=rating_min)

    if sort_by == 'title':
        movies_qs = movies_qs.order_by('title')
    elif sort_by == 'year':
        movies_qs = movies_qs.order_by('year')
    elif sort_by == '-year':
        movies_qs = movies_qs.order_by('-year')
    elif sort_by == 'rating':
        movies_qs = movies_qs.order_by('-ratings__average_rating')
    
    paginator = Paginator(movies_qs, 20)
    page_obj = paginator.get_page(request.GET.get('page'))
    all_genres = Genres.objects.values_list('genre', flat=True).distinct().order_by('genre')

    context = {
        'page_obj': page_obj,
        'all_genres': all_genres,
        'current_genre': genre_filter,
        'current_sort': sort_by
    }
    return render(request, 'movies/list.html', context)

def movie_detail(request, movie_id):
    """Détail avec Films Similaires."""
    collection = get_movies_collection()
    movie = collection.find_one({"_id": movie_id})
    
    if not movie:
        raise Http404("Film introuvable.")

    # Films similaires (Même genre)
    similar_movies = []
    if 'genres' in movie and movie['genres']:
        similar_cursor = collection.find(
            {
                "genres": {"$in": movie['genres']},
                "_id": {"$ne": movie_id}
            },
            {"title": 1, "year": 1, "rating": 1}
        ).limit(4)
        
        # CORRECTION : On transforme le curseur en liste et on renomme _id en id
        for doc in similar_cursor:
            doc['id'] = doc['_id'] # On crée un alias 'id' sans underscore
            similar_movies.append(doc)

    return render(request, 'movies/detail.html', {
        'movie': movie,
        'similar_movies': similar_movies
    })

def stats(request):
    """Stats avec Distribution des notes."""
    # 1. Genres
    genres_data = Genres.objects.values('genre').annotate(total=Count('movie_id')).order_by('-total')[:15]
    genres_labels = [item['genre'] for item in genres_data]
    genres_counts = [item['total'] for item in genres_data]

    # 2. Decennies
    years = Movies.objects.values_list('year', flat=True).exclude(year__isnull=True)
    decades = {}
    for y in years:
        decade = (y // 10) * 10
        decades[decade] = decades.get(decade, 0) + 1
    sorted_decades = sorted(decades.keys())
    decades_counts = [decades[d] for d in sorted_decades]

    # 3. Acteurs
    top_actors_data = Principals.objects.filter(category__in=['actor', 'actress']) \
        .values('person__name') \
        .annotate(count=Count('movie_id')) \
        .order_by('-count')[:10]
    actors_labels = [item['person__name'] for item in top_actors_data]
    actors_counts = [item['count'] for item in top_actors_data]

    # 4. Distribution des notes
    ratings = Ratings.objects.values_list('average_rating', flat=True).exclude(average_rating__isnull=True)
    rating_dist = {i: 0 for i in range(11)}
    for r in ratings:
        rating_dist[int(r)] += 1
    
    rating_labels = list(rating_dist.keys())
    rating_counts = list(rating_dist.values())

    context = {
        'genres_labels': genres_labels, 'genres_counts': genres_counts,
        'decades_labels': sorted_decades, 'decades_counts': decades_counts,
        'actors_labels': actors_labels, 'actors_counts': actors_counts,
        'rating_labels': rating_labels, 'rating_counts': rating_counts,
    }
    return render(request, 'movies/stats.html', context)

def search(request):
    """Recherche globale."""
    query = request.GET.get('q', '')
    movies_results = []
    persons_results = []

    if query and len(query) > 2:
        movies_results = Movies.objects.filter(title__icontains=query)[:10]
        persons_results = Persons.objects.filter(name__icontains=query)[:10]

    context = {'query': query, 'movies': movies_results, 'persons': persons_results}
    return render(request, 'movies/search.html', context)

def person_detail(request, person_id):
    """Affiche le profil d'une personne et sa filmographie (SQLite)."""
    # 1. On récupère la personne (404 si n'existe pas)
    person = get_object_or_404(Persons, person_id=person_id)

    # 2. Filmographie - Acteur (via table Principals)
    # On trie par année décroissante du film
    cast_roles = Principals.objects.filter(person=person, category__in=['actor', 'actress'])\
        .select_related('movie')\
        .order_by('-movie__year')

    # 3. Filmographie - Réalisateur (via table Directors)
    director_roles = Directors.objects.filter(person=person)\
        .select_related('movie')\
        .order_by('-movie__year')

    # 4. Filmographie - Scénariste (via table Writers)
    writer_roles = Writers.objects.filter(person=person)\
        .select_related('movie')\
        .order_by('-movie__year')

    context = {
        'person': person,
        'cast_roles': cast_roles,
        'director_roles': director_roles,
        'writer_roles': writer_roles,
    }
    return render(request, 'movies/person_detail.html', context)


def test_connection(request):
    """Vue technique pour valider la connexion MongoDB (requis T3.3)."""
    try:
        collection = get_movies_collection()
        nb_films = collection.count_documents({})
        server_info = collection.database.client.address
        html = f"<h1>OK</h1><p>{nb_films} films sur {server_info}</p>"
        return HttpResponse(html)
    except Exception as e:
        return HttpResponse(f"Erreur: {e}")

# Utilitaire
def _format_movies_for_template(queryset):
    res = []
    for r in queryset:
        if r.movie:
            res.append({
                'movie_id': r.movie.movie_id,
                'title': r.movie.title,
                'year': r.movie.year,
                'rating': r.average_rating,
                'votes': r.num_votes
            })
    return res