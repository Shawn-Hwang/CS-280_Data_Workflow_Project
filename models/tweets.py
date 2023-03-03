from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class Tweet(Base):
    __tablename__ = "tweets"
    id = Column(Integer, primary_key=True, nullable=False)
    tweet_id = Column(String, nullable=False)
    user_id = Column(String)
    text = Column(String)
    created_at = Column(Date)
    
    def __repr__(self) -> str:
        return f"Tweet(id={self.id}, tweet_id={self.tweet_id}, user_id={self.user_id}, text={self.text}, created_at={self.created_at})" 