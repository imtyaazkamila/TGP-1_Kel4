from typing import Optional
from fastapi import FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorClient

# Create FastAPI app
app = FastAPI()

# MongoDB connection
client = AsyncIOMotorClient("mongodb://localhost:27017")  # Change to your MongoDB URI if needed
db = client['movieLens']  # Use your MongoDB database (in this case, movielensDB)
movies_collection = db["movies"]
ratings_collection = db["ratings"]
links_collection = db["links"]
tags_collection = db["tags"]

# Route to get movie data by movieId
@app.get("/movies/{movie_id}")
async def get_movie(movie_id: int):
    movie = await movies_collection.find_one({"movieId": movie_id})
    if movie and "_id" in movie:
        movie["_id"] = str(movie["_id"])  # Convert ObjectId to string
        return movie
    return {"error": "Movie not found"}

@app.get("/links/{movie_id}")
async def get_movie(movie_id: int):
    links = await links_collection.find_one({"movieId": movie_id})
    if links and "_id" in links:
        links["_id"] = str(links["_id"])  # Convert ObjectId to string
        return links
    return {"error": "Links not found"}

@app.get("/ratings")
async def get_movie(limit: Optional[int] = Query(10, ge=1)):
    ratings_cursor = ratings_collection.find().limit(limit)
    ratings = await ratings_cursor.to_list(length=limit)
    if ratings:
        # Convert ObjectId to string if "_id" exists
        for rating in ratings:
            if "_id" in rating:
                rating["_id"] = str(rating["_id"])
        return ratings
    return {"error": "ratings not found"}

# Route to fetch top-rated movies based on user ratings
@app.get("/movie/top-rated")
async def get_top_rated_movies():
    movies = await ratings_collection.aggregate([
        {"$group": {"_id": "$movieId", "avgRating": {"$avg": "$rating"}}},
        {"$sort": {"avgRating": -1}},  # Sort by descending order for top ratings
        {"$limit": 10}  # Get top 10 rated movies
    ]).to_list(length=10)
    return movies
