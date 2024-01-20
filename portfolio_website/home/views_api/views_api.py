from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status 
from rest_framework import permissions
from ..models import Matches,Innings
from ..models import Players,Series
from ..models import Teams
from .. import serializers
from django.shortcuts import render
from django.db.models import F,Q


##def my_view(request):
  ##  mymatches = Matches.objects.all().using('sports_analysis')
    ##print(mymatches)
    ##context = {'mymodels': mymatches}
    #return render(request, 'api.html', {'mymatches':mymatches})

def my_view(request):
    mymatches = Matches.objects.using('sports_analysis').order_by('-match_id')[:20]
    print(mymatches)
    return render(request, 'api.html', {'mymatches': mymatches})


class MatchesListApiView(APIView):
    def get(self,request,*args,**kwargs):
        mymatches = Matches.objects.all().using('sports_analysis')
        serializer = serializers.MatchesSerializer(mymatches,many=True)
        return Response(serializer.data,status=status.HTTP_200_OK)
    

def my_view2(request):
    myplayers = Players.objects.using('sports_analysis').select_related('team').annotate(team_name_annotate=F('team__team_name')).values('player_name', 'team_name_annotate')
    print(myplayers)
    return render(request, 'api.html', {'myplayers': myplayers})

def my_view3(request):
    teams = Teams.objects.using('sports_analysis').order_by('team_name')
    print(teams)
    return render(request, 'project.html', {'teams': teams})

def my_view4(request): 
    queryset = Series.objects.using('sports_analysis').filter(
        seriesmatches__match__winning_team__isnull=False
    ).annotate(
        winning_team=F('seriesmatches__match__winning_team__team_name'),
        match_type=F('seriesmatches__match__match_type'),
        venue=F('seriesmatches__match__venue'),
        city=F('seriesmatches__match__city')
    ).values(
        'series_name', 'series_start_date', 'series_end_date',
        'match_type', 'venue', 'city', 'winning_team'
    ).distinct()
    return render(request, 'project2.html', {'queryset': queryset})



# The updated filter


    
