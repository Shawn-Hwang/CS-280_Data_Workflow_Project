from config import Session #You would import this from your config file
from users import User
from datetime import datetime

session = Session()
user = User(
            user_id=112345,
            username="CrazyCatLady1001",
            name="Mariana Ramierez",
            created_at=datetime.now()
          )

# Add the user
session.add(user)

#Commit your changes to the database
session.commit()

#Close the connection
session.close()