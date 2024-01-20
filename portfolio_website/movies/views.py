from django.shortcuts import render,HttpResponse
from . models import Movies
# Create your views here.
def movies(request):
    moviesstr = Movies.objects.all() #queryset containing all movies we just created
    return render(request=request, template_name="movies.html", context={'movies':moviesstr})
    #return HttpResponse("<h1> hsjbc </h1>")