package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;

import java.util.*;

import com.mongodb.client.*;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class MongodbDatabase extends AbstractDatabase {

    MongoClient mongoClient;
    MongoDatabase database;

    public MongodbDatabase() {
        // start connection to database
        this.mongoClient = MongoClients.create("mongodb://localhost:27017");
        this.database = mongoClient.getDatabase("MovieLens");
    }

    @Override
    public List<Movie> getAllMovies() {
        List<Movie> movies = new LinkedList<Movie>();

        MongoCollection<Document> moviesCollection = database.getCollection("movies");
        FindIterable<Document> moviesItems = moviesCollection.find();

        for (Document doc : moviesItems) {
            movies.add(new Movie(doc.getInteger("_id"), doc.getString("title"), getMovieTypes(doc)));
        }

        return movies;
    }

    public List<Genre> getMovieTypes(Document movieDoc) {
        List<Genre> genres = new ArrayList<>();
        List<String> readGenres = movieDoc.getList("genres", String.class);

        for (int i = 0; i < readGenres.size(); i++) {
            genres.add(new Genre(i, readGenres.get(i)));
        }

        return genres;
    }

    public Document getDocumentFromCollection(String collectionName, String fieldToBeSearched, Object fieldValue) {
        MongoCollection<Document> dbCollection = database.getCollection(collectionName);
        FindIterable<Document> docItem = dbCollection.find(eq(fieldToBeSearched, fieldValue));

        return docItem.first();
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        List<Movie> movies = new LinkedList<Movie>();

        Document user = getDocumentFromCollection("users", "_id", userId);
        List<Document> readMovies = user.getList("movies", Document.class);

        for (Document readMovie : readMovies) {
            Document movie = getDocumentFromCollection("movies", "_id", readMovie.get("movieid"));
            movies.add(new Movie(movie.getInteger("_id"), movie.getString("title"), getMovieTypes(movie)));
        }

        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        List<Rating> ratings = new LinkedList<Rating>();

        Document user = getDocumentFromCollection("users", "_id", userId);
        List<Document> readMovies = user.getList("movies", Document.class);

        for (Document readMovie : readMovies) {
            Document movie = getDocumentFromCollection("movies", "_id", readMovie.get("movieid"));
            ratings.add(new Rating(new Movie(movie.getInteger("_id"), movie.getString("title"), getMovieTypes(movie)), userId, readMovie.getInteger("rating")));
        }

        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        MongoCollection<Document> usersCollection = database.getCollection("users");
        Document user = getDocumentFromCollection("users", "_id", rating.getUserId());

        Document userUpdated = new Document("_id", user.getInteger("_id"));
        userUpdated.append("name", user.getString("name"));
        userUpdated.append("gender", user.getString("gender"));
        userUpdated.append("age", user.getInteger("age"));
        userUpdated.append("occupation", user.getString("occupation"));

        boolean isExistant = false;
        List<Document> newMoviesList = new LinkedList<Document>();
        for (Document readMovie : user.getList("movies", Document.class)) {
            Document newMovieItem = new Document("movieid", readMovie.getInteger("movieid"));
            newMovieItem.append("rating", readMovie.getInteger("rating"));

            if ((rating.getMovieId() == readMovie.getInteger("movieid")) &&
                    (rating.getScore() == readMovie.getInteger("rating"))) {
                newMovieItem.put("rating", rating.getScore());
                isExistant = true;
            }

            newMoviesList.add(newMovieItem);
        }

        if (!isExistant) {
            Document newMovieRate = new Document("movieid", rating.getMovieId());
            newMovieRate.append("rating", rating.getScore());
            newMoviesList.add(newMovieRate);
        }

        userUpdated.append("movies", newMoviesList);

        Document set = new Document("$set", new Document(userUpdated));
        usersCollection.updateOne(user, set);
    }

    public boolean containsRating(final List<Rating> list, final Rating rate){
        return list.stream().anyMatch(o -> o.equals(rate));
    }

    private int compareListOfMovies(List<Rating> list1, List<Rating> list2) {
        int numResemblance = 0;
        for (Rating r : list1) {
            if (this.containsRating(list2, r)) {
                numResemblance++;
            }
        }

        return numResemblance;
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        MongoCollection<Document> usersCollection = database.getCollection("movies");
        FindIterable<Document> users = usersCollection.find();

        List<Rating> moviesRatedByUser = getRatingsFromUser(userId);
        Document theUserClose = null;
        int numOfMoviesSame = 0;

        for (Document user : users) {
            List<Rating> moviesRated = this.getRatingsFromUser(user.getInteger("_id"));
            int occurrences = this.compareListOfMovies(moviesRatedByUser, moviesRated);
            if (occurrences > numOfMoviesSame) {
                numOfMoviesSame = occurrences;
                theUserClose = user;
            }
        }

        assert theUserClose != null;
        return this.getRatingsFromUser(theUserClose.getInteger("_id"));
    }    
}
