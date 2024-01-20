from movies.models import Movies
class MoviesRouter:
    route_app_labels = {Movies}

    def db_for_read(self,model,**hints):
        if model in self.route_app_labels:
            return "movies"
        return None
    
    def db_for_write(self, model, **hints):
        if model in self.route_app_labels:
            return "movies"
        return None
    
    def allow_relation(self, obj1, obj2, **hints):
        "Allow any relation if a both models in chinook app"
        if obj1 in self.route_app_labels  and obj2 in self.route_app_labels :
            return True
        # Allow if neither is chinook app
        elif 'movies' not in [obj1._meta.app_label, obj2._meta.app_label]: 
            return True
        return False
    
    def allow_syncdb(self, db, model):
        if db == 'movies' or model._meta.app_label == "movies":
            return False # we're not using syncdb on our legacy database
        else: # but all other models/databases are fine
            return True
