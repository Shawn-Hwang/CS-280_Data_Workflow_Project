from config import Session #You would import this from your config file
from users import User
from datetime import datetime

session = Session()

# This will retrieve all of the users from the database 
# (It'll be a list, so you may have 100 users or 0 users)
users_lst = session.query(User).all()
user_ids = [u.user_id for u in users_lst]
print(user_ids)

# This will retrieve the user who's username is NASA
nasaUser = session.query(User).filter(User.username == "POTUS").first()

#You can then print the username of the user you retrieved
print(nasaUser.name)

#We recommend that you reassign the user to a variable so that you can use it later
nasaUsername = nasaUser.username

# This will close the session that you opened at the beginning of the file.
session.close()