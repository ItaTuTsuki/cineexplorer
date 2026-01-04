"""
URL configuration for config project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path
from movies import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.home, name='home'),             # Accueil
    path('movies/', views.movie_list, name='movie_list'), # Liste 
    path('stats/', views.stats, name='stats'),     # Stats 
    path('search/', views.search, name='search'),   # Recherche 
    path('movies/<str:movie_id>/', views.movie_detail, name='movie_detail'),
    path('person/<str:person_id>/', views.person_detail, name='person_detail'),

    path('test/', views.test_connection),
]
