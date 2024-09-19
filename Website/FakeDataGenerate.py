import random
import datetime
import bcrypt
import pymongo

MONGO_URI = ""
MONGO_DB = "netflix_DB"
MONGO_COLLECTION = "users"

client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

FIRST_NAMES = ["John", "Jane", "Michael", "Emily", "David", "Sophia", "Daniel", "Olivia", "Matthew", "Isabella","Minatozaki","Chou","Kang"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Davis", "Miller", "Wilson", "Anderson", "Taylor", "Thomas","Sana","Tzuyu","Haerin"]

tv_show_keywords = [ "Breaking Bad", "Game of Thrones", "Stranger Things", "Friends", "The Office", "Better Call Saul", "The Crown", "The Mandalorian", "Sherlock", "The Sopranos", "Westworld", "Narcos", "House of Cards", "Fargo", "The Witcher", "The Walking Dead", "True Detective", "Rick and Morty", "The Big Bang Theory", "Chernobyl", "Peaky Blinders", "Black Mirror", "The Boys", "BoJack Horseman", "Mindhunter"]
movie_keywords = [ "Inception", "The Dark Knight", "Fight Club", "Titanic", "Forrest Gmp", "Avengers Endgame", "Pulp Fiction", "Schindler's List", "The Matrix", "Lord of the Rings", "Shawshank Redmption", "The Godfthr", "Star Wars", "Jurassic Prk", "The Social Network", "Interstellar", "The Lion King", "Pirates of Caribean", "Spider-Man", "Back to the Futur", "Mad Max", "Goodfellas", "Saving Private Ryan", "Toy Story", "Guardians of Galaxy"]
person_keywords = [ "Leonardo DiCaprio", "Johnny Dep", "Scarlett Johansson", "Tom Hanks mov", "Angelina Jolie", "Will Smth", "Chris Evans", "Robert Downey", "Emma Stone", "Dwayne Jhonson", "Jennifer Aniston", "Brad Pitt", "Natalie Portmn", "Chris Hmsworth", "Samuel L. Jacksn", "Meryl Stree", "Kate Winslet", "Keanu Reev", "Tom Cruise", "Robert Pattinsn", "Ryan Gosling", "Margot Robbie", "Hugh Jackmn", "Anne Hathaway", "Christian Bale"]
proType = ["tv","movie","person"]

def generateFakeData(num_document):
    for item in range(num_document):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        email = f"{first_name.lower()}.{last_name.lower()}@gmail.com"

        password = "".join(random.choices("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=12))
        hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

        image = f"/avatar{random.randint(1, 3)}.png"

        search_history = []
        for _ in range(random.randint(5, 20)):
            search_type = random.choice(["tv", "movie", "person"])
            if search_type == "tv":
                search_keyword = random.choice(tv_show_keywords)
            elif search_type == "movie":
                search_keyword = random.choice(movie_keywords)
            else:
                search_keyword = random.choice(person_keywords)

            search_history.append({
                "id": random.randint(1000000, 2000000),
                "image": f"/lhKIArhDJ82unUgt83VCzqtjkGZ.jpg",
                "title": "None",
                "searchType": search_type,
                "createdAt": datetime.datetime.now(),
                "keyword": search_keyword
            })

        document = {
            "username": f"{first_name.lower()}{last_name.lower()}",
            "email": email,
            "password": hashed_password,
            "image": image,
            "searchHistory": search_history
        }

        print(document)
        collection.insert_one(document)
        print("IMPORTED")

generateFakeData(5)