from django.contrib import admin
from django.urls import path, include
from . import views
from .views_api import views_api

urlpatterns = [
    path("admin/", admin.site.urls),
    path("",views.home, name='home'),
    path("about/",views.about, name='about'),
    #path("project/",views.project, name='project'),
    path("contact/",views.contact, name='contact'),
    path("sets",views.movies, name='movies'),
    #path("api.html",views_api.my_view,name='my_view'),
    path("api/",views_api.MatchesListApiView.as_view(),name='MatchesListApiView'),
    path("api.html",views_api.my_view2,name='my_view2'),
    path("project.html/",views_api.my_view3, name='my_view3'),
    #path("home.html/",views_api.my_view4, name='my_view4'),
    path("project2.html/",views_api.my_view4, name='my_view4')


]



