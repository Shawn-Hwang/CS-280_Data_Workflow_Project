from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class TweetTimeSeries(Base):
    __tablename__ = "tweet_timeseries"
    id = Column(Integer, primary_key=True, nullable=False)
    tweet_id = Column(String, nullable=False)
    retweet_count = Column(Integer)
    favorite_count = Column(Integer)
    date = Column(Date)
    
    def __repr__(self) -> str:
        return f"TweetTimeSeries(id={self.id}, user_id={self.user_id}, nt={self.following_count}, retweet_count={self.retweet_count}, \
                 favorite_count={self.favorite_count}, date={self.date})" 