import random
import datetime
import pymongo
from bson import ObjectId
import time

MONGO_URI = ""
MONGO_DB = "netflix_DB"
MONGO_COLLECTION = "searchhistories"

client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

tv_show_keywords = ["Breaking Bad", "Game of Thrones", "Stranger Things", "Friends", "The Office", "Better Call Saul", "The Crown", "The Mandalorian", "Sherlock", "The Sopranos", "Westworld", "Narcos", "House of Cards", "Fargo", "The Witcher", "The Walking Dead", "True Detective", "Rick and Morty", "The Big Bang Theory", "Chernobyl", "Peaky Blinders", "Black Mirror", "The Boys", "BoJack Horseman", "Mindhunter"]
movie_keywords = ["Inception", "The Dark Knight", "Fight Club", "Titanic", "Forrest Gump", "Avengers: Endgame", "Pulp Fiction", "Schindler's List", "The Matrix", "Lord of the Rings", "Shawshank Redemption", "The Godfather", "Star Wars", "Jurassic Park", "The Social Network", "Interstellar", "The Lion King", "Pirates of the Caribbean", "Spider-Man", "Back to the Future", "Mad Max", "Goodfellas", "Saving Private Ryan", "Toy Story", "Guardians of the Galaxy"]
person_keywords = ["Leonardo DiCaprio", "Johnny Depp", "Scarlett Johansson", "Tom Hanks", "Angelina Jolie", "Will Smith", "Chris Evans", "Robert Downey Jr.", "Emma Stone", "Dwayne Johnson", "Jennifer Aniston", "Brad Pitt", "Natalie Portman", "Chris Hemsworth", "Samuel L. Jackson", "Meryl Streep", "Kate Winslet", "Keanu Reeves", "Tom Cruise", "Robert Pattinson", "Ryan Gosling", "Margot Robbie", "Hugh Jackman", "Anne Hathaway", "Christian Bale"]

def generate_fake_data(num_user):
    total_documents = 0
    for _ in range(num_user):
        user_id = ObjectId()
        searches = random.randint(5, 20)  

        for _ in range(searches):
            search_type = random.choice(["tv", "movie", "person"])
            if search_type == "tv":
                keyword = random.choice(tv_show_keywords)
            elif search_type == "movie":
                keyword = random.choice(movie_keywords)
            else:
                keyword = random.choice(person_keywords)

            document = {
                "_id": ObjectId(),
                "userId": user_id,
                "id": random.randint(1000, 2000),
                "image": f"/{''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=20))}.jpg",
                "title": keyword,
                "searchType": search_type,
                "keyword": keyword.lower(),
                "createdAt": datetime.datetime.now()
            }

            collection.insert_one(document)
            total_documents += 1
            print("IMPORTED -", document)

    print(f"Generated and imported {total_documents} documents.")

status = "ON"
try:
    while status == "ON":
        generate_fake_data(num_user=random.randint(1, 2))
        time.sleep(60)
except KeyboardInterrupt:
    print("Data generation stopped by user.")
finally:
    client.close()
    print("MongoDB connection closed.")
