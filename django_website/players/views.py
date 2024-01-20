from django.shortcuts import render
from django.http import HttpResponse
# Create your views here.

def index(request):
    return HttpResponse("<H2> Hey </H2>")

def say_hello(request):
    #return HttpResponse("<H2> Hello </H2>")
    x=1 
    y=2
    return render(request,'hello.html',{"name":"Nitin"})


