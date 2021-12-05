package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;

import org.neo4j.driver.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Neo4jDatabase extends AbstractDatabase {
    private final Driver driver;
    private Session session;

    public Neo4jDatabase(String uri, String user, String password) {
        // start connection to database
        this.driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
        this.session = driver.session();
    }

    HashMap<Integer, Genre> getAllGenres() {
        HashMap<Integer, Genre> allGenres = new HashMap<>();
        String selectGenresRequest = "MATCH (g:Genre)" +
                "RETURN g.id, g.name" ;
        Result res = this.session.run(selectGenresRequest);
        while (res.hasNext()) {
            Record record = res.next();
            int genreId = record.get("g.id").asInt();
            String genreName = record.get("g.name").asString();
            allGenres.put(genreId, new Genre(genreId, genreName));
        }

        return allGenres;
    }

    @Override
    public List<Movie> getAllMovies() {
        HashMap<Integer, Genre> allGenres = getAllGenres();
        List<Movie> movies = new LinkedList<Movie>();
        String selectMoviesRequest = "MATCH (m:Movie)-[:CATEGORIZED_AS]->(g:Genre)"+
                "RETURN m.id as movie_id, m.title as movie_title, collect(g.id) as genre_id";
        Result res = session.run(selectMoviesRequest);
        while (res.hasNext()) {
            Record record = res.next();
            int movieId = record.get("movie_id").asInt();
            String movieTitle = record.get("movie_title").asString();
            List<Object> genreIds = record.get("genre_id").asList();
            List<Genre> movieGenres = new LinkedList<>();
            for (Object genreId : genreIds) {
                movieGenres.add(allGenres.get((int)(long)genreId));
            }
            movies.add(new Movie(movieId, movieTitle, movieGenres));
        }

        return movies;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        HashMap<Integer, Genre> allGenres = getAllGenres();
        List<Movie> movies = new LinkedList<Movie>();
        String selectRatedMoviesByUserRequest = "MATCH (u1:User {id:"+userId+"})-[r:RATED]->(m:Movie), (m:Movie)-[:CATEGORIZED_AS]->(g:Genre)" +
                "RETURN m.id as movie_id, m.title as movie_title, collect(g.id) as genre_id";
        Result res = session.run(selectRatedMoviesByUserRequest);
        while (res.hasNext()) {
            Record record = res.next();
            int movieId = record.get("movie_id").asInt();
            String movieTitle = record.get("movie_title").asString();
            List<Object> genreIds = record.get("genre_id").asList();
            List<Genre> movieGenres = new LinkedList<>();
            for (Object genreId : genreIds) {
                movieGenres.add(allGenres.get((int)(long)genreId));
            }
            movies.add(new Movie(movieId, movieTitle, movieGenres));
        }
        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        HashMap<Integer, Genre> allGenres = getAllGenres();
        List<Rating> ratings = new LinkedList<Rating>();
        String selectMoviesSQL = "MATCH (u1:User {id:"+userId+"})-[r:RATED]->(m:Movie), (m:Movie)-[:CATEGORIZED_AS]->(g:Genre)" +
                "RETURN m.id as movie_id, m.title as movie_title, r.note as rating, collect(g.id) as genre_id";
        Result res = session.run(selectMoviesSQL);
        while (res.hasNext()) {
            Record record = res.next();
            int movieId = record.get("movie_id").asInt();
            String movieTitle = record.get("movie_title").asString();
            List<Object> genreIds = record.get("genre_id").asList();
            int rating = record.get("rating").asInt();
            List<Genre> movieGenres = new LinkedList<>();
            for (Object genreId : genreIds) {
                movieGenres.add(allGenres.get((int)(long)genreId));
            }
            ratings.add(new Rating(new Movie(movieId, movieTitle, movieGenres), userId, rating));;
        }
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        String checkIfRelationExist = "MATCH (u:User {id:" + rating.getUserId() + "})-[r:RATED]->(m:Movie {id:" + rating.getMovieId() + "}) " +
                "RETURN r";
        Result resultRelationCheck = this.session.run(checkIfRelationExist);
        if (resultRelationCheck.hasNext()) {    // Means that the user has already rated the movie, so we update the rating score
            String UpdateMovieRatingReq= "MATCH (u:User { id:" + rating.getUserId() + "})-[r:RATED]->(m:Movie {id:" + rating.getMovieId() + "}) "+
                    "SET r.note =" + rating.getScore() + " " +
                    "RETURN r";
            this.session.run(UpdateMovieRatingReq);
        } else {
            String AddMovieRatingReq= "MATCH (u: User { id:" + rating.getUserId() + "}), (m:Movie  { id: " + rating.getMovieId() + "})\n" +
                    "CREATE (u)-[r:RATED { note: " + rating.getScore() + " }]->(m)  \n" +
                    "RETURN r";
            this.session.run(AddMovieRatingReq);
        }
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        List<Rating> ratings = new LinkedList<Rating>();
        String selectMoviesToRecommend = "MATCH (target_user:User {id : 55})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User) \n" +
                "WITH other_user, count(distinct m.title) AS num_common_movies, target_user\n" +
                "ORDER BY num_common_movies DESC\n" +
                "LIMIT 1\n" +
                "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie)\n" +
                "WHERE NOT (target_user)-[:RATED]->(m2)\n" +
                "RETURN m2.id AS movie_id, m2.title AS rec_movie_title, rat_other_user.note AS rating, other_user.id AS watched_by\n" +
                "ORDER BY rat_other_user.note DESC;";
        Result res = session.run(selectMoviesToRecommend);

        String titlePrefix;
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");

        while (res.hasNext()) {
            Record record = res.next();
            if (processingMode == 0) {
                titlePrefix = "0_";
            } else if (processingMode == 1) {
                titlePrefix = "1_";
            } else if (processingMode == 2) {
                titlePrefix = "2_";
            } else {
                titlePrefix = "default_";
            }
            int movieId = record.get("movie_id").asInt();
            String movieTitle = record.get("rec_movie_title").asString();
            int rating = record.get("rating").asInt();
            ratings.add(new Rating(new Movie(movieId, titlePrefix + movieTitle, Arrays.asList(genre0, genre1)), userId, rating));
        }

        return ratings;
    }
}
