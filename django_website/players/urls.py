from django.urls import path,include
from . import views 
import debug_toolbar
urlpatterns = [
    path('',views.index,name='index'),
    path('hello/',views.say_hello),
    path("__debug__/",include(debug_toolbar.urls))
]
