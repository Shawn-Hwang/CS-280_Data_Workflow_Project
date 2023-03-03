# from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def create_dburl():
    # hostname = Variable.get("DATABASE_USERNAME")
    # username = Variable.get("DATABASE_PASSWORD")
    # password = Variable.get("DATABASE_HOSTNAME")
    # port = Variable.get("DATABASE_PORT")
    # database = Variable.get("DATABASE_NAME")
    hostname = '0.0.0.0'
    username = 'postgres'
    password = 12345
    port = 5432
    database = 'twitter'
    return f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}"

engine = create_engine(create_dburl())
Session = sessionmaker(bind=engine)