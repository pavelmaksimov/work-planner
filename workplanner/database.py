import databases
import sqlalchemy

from workplanner.settings import Settings

db = databases.Database(str(Settings().dbpath))
metadata = sqlalchemy.MetaData()
engine = sqlalchemy.create_engine(
    str(Settings().dbpath), connect_args={"check_same_thread": False}
)
metadata.create_all(engine)
