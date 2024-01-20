from rest_framework import serializers
from .models import Matches

class MatchesSerializer(serializers.ModelSerializer):
    class Meta:
        model =  Matches
        fields = ["match_id","match_type","series_id","match_date","venue","city","toss_winner","toss_decision","result","winning_team"]