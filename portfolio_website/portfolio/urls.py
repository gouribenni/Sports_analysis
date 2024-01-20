"""portfolio URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
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
from django.urls import path, include
from django.shortcuts import render, HttpResponse
from home import views
from django.conf import settings #add this
from django.conf.urls.static import static #add this
urlpatterns = [
    path('api-auth/',include('rest_framework.urls')),
    path("trust/",include("movies.urls")) ,
    path("admin/", admin.site.urls),
    path("",include("home.urls")) ,
    path("about/",include("home.urls")),
    path("project/",include("home.urls")) ,
    path("contact/",include("home.urls")) ,  
    path("test_matches/",include("home.urls"))           
]

if settings.DEBUG: #add this
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

