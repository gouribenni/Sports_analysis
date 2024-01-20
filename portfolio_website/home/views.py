from django.shortcuts import render, HttpResponse
# Create your views here.
def home(request):
    #return HttpResponse("This is my homepage (/)")
    context = {'name':'harry','course':'Django'}
    return render(request,"home.html",context)

def about(request):
    #return HttpResponse("This is my about page (/)")
    return render(request,"about.html")

def project(request):
    #return HttpResponse("This is my project page (/)")
    return render(request,"project.html")
def contact(request):
    #return HttpResponse("This is my contact page (/)")
    return render(request,"contact.html")

def movies(request):
    #moviesstr = Movies.objects.all() #queryset containing all movies we just created
    #return render(request=request, template_name="main/movies.html", context={'movies':movies})
    return HttpResponse("<h1> hsjbcsxcsccs </h1>")


