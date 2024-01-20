from django.contrib import admin
from django.urls import path, include
from . import views
from django.conf import settings #add this
from django.conf.urls.static import static #add this
urlpatterns = [
    path("sets/",views.movies, name='movies'),
]

if settings.DEBUG: #add this
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)