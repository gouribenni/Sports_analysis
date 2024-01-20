import os.path, sys

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
os.environ["PATH"] += os.getcwd()
POSTGRES_HOST = "ec2-100-25-135-202.compute-1.amazonaws.com"
ENGINE= 'django.db.backends.postgresql_psycopg2'
# LOCAL_POSTGRES_HOST = "server1.navicat.com"
DATABASE_NAME = "sports_analysis"
POSTGRES_HOST = "localhost"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "sports_analysis"
AWS_CREDNTIALS_FILE = "/Users/nyzy/.aws/credentials"
JSON_FILE_DIR = "/Users/gouribenni/COURSES SEM 2/BIG DATA/PROJECTS/Data/india_male_json"
S3_FILE_DIR = "s3://sports-analysis-project/raw_files/india_male_json/"

