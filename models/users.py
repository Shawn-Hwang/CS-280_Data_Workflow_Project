from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, not_null=True)
    user_id = Column(Integer, not_null=True)
    username = Column(String, not_null=True)
    name = Column(String, not_null=True)
    created_at = Column(Date, not_null=True)
    
    def __repr__(self) -> str:
        return f"User(id={self.id}, user_id={self.user_id}, username={self.username}, name={self.name}, created_at={self.created_at})" 