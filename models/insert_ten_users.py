from config import Session #You would import this from your config file
from users import User
from datetime import datetime

session = Session()
user_0 = User(
            user_id=5162861,
            username="NintendoAmerica",
            name="Nintendo of America",
            created_at=datetime.now()
          )

user_1 = User(
            user_id=10671602,
            username="PlayStation",
            name="PlayStation",
            created_at=datetime.now()
          )

user_2 = User(
            user_id=24742040,
            username="Xbox",
            name="Xbox",
            created_at=datetime.now()
          )

user_3 = User(
            user_id=11348282,
            username="NASA",
            name="NASA",
            created_at=datetime.now()
          )

user_4 = User(
            user_id=1349149096909668363,
            username="POTUS",
            name="President Biden",
            created_at=datetime.now()
          )

user_5 = User(
            user_id=34743251,
            username="SpaceX",
            name="SpaceX",
            created_at=datetime.now()
          )

user_6 = User(
            user_id=44196397,
            username="elonmusk",
            name="Elon Musk",
            created_at=datetime.now()
          )

user_7 = User(
            user_id=17600950,
            username="BYU",
            name="BYU",
            created_at=datetime.now()
          )

user_8 = User(
            user_id=4398626122,
            username="OpenAI",
            name="OpenAI",
            created_at=datetime.now()
          )

user_9 = User(
            user_id=2905440476,
            username="alexa99",
            name="Alexa",
            created_at=datetime.now()
          )

# Add the user
users = [user_0, user_1, user_2, user_3, user_4, user_5, user_6, user_7, user_8, user_9]
session.add_all(users)

# Commit your changes to the database
session.commit()

# Close the connection
session.close()