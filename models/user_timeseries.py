from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class UserTimeSeries(Base):
    __tablename__ = "user_timeseries"
    id = Column(Integer, primary_key=True, not_null=True)
    user_id = Column(Integer, not_null=True)
    followers_count = Column(Integer)
    following_count = Column(Integer)
    tweet_count = Column(Integer)
    listed_count = Column(Integer)
    date = Column(Date)
    
    def __repr__(self) -> str:
        return f"UserTimeSeries(id={self.id}, user_id={self.user_id}, followers_count={self.followers_count}, following_count={self.following_count}, \
                 tweet_count={self.tweet_count}, listed_count={self.listed_count}, date={self.date})" 