from django.db import models

# Create your models here.
class Teacher(models.Model):
    name = models.CharField(max_length=80)
    age = models.IntegerField()


# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Deliveries(models.Model):
    delivery_id = models.BigAutoField(primary_key=True)
    match_id = models.IntegerField()
    inning_id = models.IntegerField(blank=True, null=True)
    batsman_id = models.IntegerField()
    bowler_id = models.IntegerField()
    non_striker_id = models.IntegerField()
    ball_number = models.IntegerField(blank=True, null=True)
    over_number = models.IntegerField(blank=True, null=True)
    runs_scored = models.IntegerField(blank=True, null=True)
    extra_runs = models.IntegerField(blank=True, null=True)
    is_wicket = models.BooleanField(blank=True, null=True)
    player_out = models.CharField(max_length=255, blank=True, null=True)
    dismissal_type = models.CharField(max_length=255, blank=True, null=True)
    fielder_id = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'deliveries'
        unique_together = (('delivery_id', 'batsman_id', 'bowler_id', 'non_striker_id', 'match_id'),)


class Innings(models.Model):
    innings = models.OneToOneField('MatchesInnings', models.DO_NOTHING)
    batting_team = models.OneToOneField('Teams', models.DO_NOTHING, primary_key=True,related_name='+')
    bowling_team = models.ForeignKey('Teams', models.DO_NOTHING,related_name='+')
    total_runs = models.IntegerField(blank=True, null=True)
    wickets_lost = models.IntegerField(blank=True, null=True)
    innings_number = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'innings'
        unique_together = (('batting_team', 'bowling_team', 'innings'),)


class Matches(models.Model):
    match_id = models.BigIntegerField(primary_key=True)
    match_type = models.CharField(max_length=255, blank=True, null=True)
    series_id = models.BigIntegerField(blank=True, null=True)
    match_date = models.DateField(blank=True, null=True)
    venue = models.CharField(max_length=255, blank=True, null=True)
    city = models.CharField(max_length=255, blank=True, null=True)
    toss_winner = models.BigIntegerField()
    toss_decision = models.CharField(max_length=255, blank=True, null=True)
    result = models.CharField(max_length=255, blank=True, null=True)
    winning_team = models.ForeignKey('Teams', models.DO_NOTHING, db_column='winning_team', blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'matches'


class MatchesInnings(models.Model):
    inning_id = models.AutoField(primary_key=True)
    match = models.ForeignKey(Matches, models.DO_NOTHING, blank=True, null=True)
    innings_number = models.IntegerField(blank=True, null=True)
    team_batted = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'matches_innings'


class PlayerStats(models.Model):
    player = models.OneToOneField('Players', models.DO_NOTHING, primary_key=True)
    matches_played = models.IntegerField(blank=True, null=True)
    runs_scored = models.IntegerField(blank=True, null=True)
    balls_faced = models.IntegerField(blank=True, null=True)
    strike_rate = models.FloatField(blank=True, null=True)
    hundreds = models.IntegerField(blank=True, null=True)
    fifties = models.IntegerField(blank=True, null=True)
    best_score = models.IntegerField(blank=True, null=True)
    wickets_taken = models.IntegerField(blank=True, null=True)
    economy = models.FloatField(blank=True, null=True)
    five_wickets = models.IntegerField(blank=True, null=True)
    ten_wickets = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'player_stats'


class Players(models.Model):
    player_id = models.BigAutoField(primary_key=True)
    player_name = models.CharField(max_length=255, blank=True, null=True)
    team = models.ForeignKey('Teams', models.DO_NOTHING)
    team_name = models.ForeignKey('Teams', models.DO_NOTHING, db_column='team_name', to_field='team_name', blank=True, null=True,related_name='+')
    date_of_birth = models.DateField(blank=True, null=True)
    batting_style = models.CharField(max_length=255, blank=True, null=True)
    bowling_style = models.CharField(max_length=255, blank=True, null=True)
    player_role = models.CharField(max_length=255, blank=True, null=True)
    player_type = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'players'


class Series(models.Model):
    series_id = models.BigAutoField(primary_key=True)
    series_name = models.CharField(max_length=255, blank=True, null=True)
    season = models.CharField(max_length=255, blank=True, null=True)
    match_id_set_text = models.TextField(blank=True, null=True)  # This field type is a guess.
    series_start_date = models.DateField(blank=True, null=True)
    series_end_date = models.DateField(blank=True, null=True)
    winner = models.ForeignKey('Teams', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'series'


class SeriesMatches(models.Model):
    series = models.ForeignKey(Series, models.DO_NOTHING)
    match = models.OneToOneField(Matches, models.DO_NOTHING, primary_key=True)
    team_1 = models.ForeignKey('Teams', models.DO_NOTHING, db_column='team_1',related_name='+')
    team_2 = models.ForeignKey('Teams', models.DO_NOTHING, db_column='team_2',related_name='+')
    match_number = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'series_matches'
        unique_together = (('match', 'series', 'team_1', 'team_2'), ('match', 'team_1', 'team_2'),)


class TeamStats(models.Model):
    team = models.OneToOneField('Teams', models.DO_NOTHING, primary_key=True)
    matches_played = models.IntegerField(blank=True, null=True)
    matches_won = models.IntegerField(blank=True, null=True)
    matches_lost = models.IntegerField(blank=True, null=True)
    matches_tied = models.IntegerField(blank=True, null=True)
    matches_drawn = models.IntegerField(blank=True, null=True)
    total_runs_scored = models.IntegerField(blank=True, null=True)
    total_wickets_lost = models.IntegerField(blank=True, null=True)
    total_overs_bowled = models.IntegerField(blank=True, null=True)
    total_overs_faced = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'team_stats'


class Teams(models.Model):
    team_id = models.BigAutoField(primary_key=True)
    team_name = models.CharField(unique=True, max_length=255)
    team_short_name = models.CharField(max_length=255, blank=True, null=True)
    team_logo_url = models.CharField(max_length=255, blank=True, null=True)
    team_captain = models.BigIntegerField(blank=True, null=True)
    team_coach = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'teams'
