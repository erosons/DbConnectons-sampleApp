import sqlite3  # sqlite3 is a lite db used for most phone apps and tables apps
import json
from pathlib import Path

#  A simpler way to load a load a json lite into a dictionary
Movie = json.loads(Path("movies.json").read_text())
print(Movie)

# To connect to the data base
# the reason for thw with is to handle the close method
with sqlite3.connect("db.sqlite3") as conn:
    # insert into our database
    Command = "CREATE TABLE if not exists Movie(id int(10),title varchar(20),genre varchar(20),year int(20))"
    Command1 = "INSERT INTO Movie Values(?,?,?,?)"
    # you need to iterate over the data to be passed into the database in line 6
    for movies in Movie:
        # call the method that excute the SQL command and also execute the insertion.
        conn.execute(Command)
        conn.execute(Command1, tuple(movies.values()))
    conn.commit()

# selecting data from a database
with sqlite3.connect("db.sqlite3") as conn:
    # selecting data from a database
    Command = "SELECT * FROM Movie"
    cursor = conn.execute(Command)  # the cursor is an iterable objects
    # for row in cursor:
    # print(row)
    # This returns entire rows in the database, Note that after after you loop over an  iteration, you are at the end of the data
    allrows = cursor.fetchall()
    # to call such data again you have to connect out the For loop else you with always get an empty list. tuplr returned
    print(allrows)
