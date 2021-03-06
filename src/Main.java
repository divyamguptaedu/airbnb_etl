import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.nio.charset.*;

public class Main {

    static String DB_URL = "jdbc:mysql://localhost/";
    static final String USER = "root";
    static final String PASSWORD = "Kuch_bh1!";

    public static void createDatabase(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            Statement statement = conn.createStatement()
        ) {
            // create
            String create = "CREATE DATABASE airbnb";
            statement.executeUpdate(create);
            System.out.println("Database created successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void addListing(String[] args) {
        DB_URL = "jdbc:mysql://localhost/airbnb";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement statement = conn.createStatement()
        ) {
            // add listing table
            String createListing = "CREATE TABLE listing " +
                    "(id INTEGER not NULL, " +
                    " listing_url VARCHAR(500), " +
                    " scrape_id VARCHAR(500), " +
                    " last_scraped DATE, " +
                    " name VARCHAR(50), " +
                    " description VARCHAR(500), " +
                    " neighborhood_overview VARCHAR(500), " +
                    " picture_url VARCHAR(500), " +
                    " host_id INTEGER, " +
                    " host_url VARCHAR(500), " +
                    " host_name VARCHAR(50), " +
                    " host_since DATE, " +
                    " host_location VARCHAR(50), " +
                    " host_about VARCHAR(500), " +
                    " host_response_time VARCHAR(50), " +
                    " host_response_rate VARCHAR(4), " +
                    " host_acceptance_rate VARCHAR(4), " +
                    " host_is_superhost VARCHAR(1), " +
                    " host_thumbnail_url VARCHAR(500), " +
                    " host_picture_url VARCHAR(500), " +
                    " host_neighbourhood VARCHAR(50), " +
                    " host_listings_count INTEGER, " +
                    " host_total_listings_count INTEGER, " +
                    " host_verifications VARCHAR(500), " +
                    " host_has_profile_pic VARCHAR(1), " +
                    " host_identity_verified VARCHAR(1), " +
                    " neighbourhood VARCHAR(50), " +
                    " neighbourhood_cleansed VARCHAR(50), " +
                    " neighbourhood_group_cleansed VARCHAR(50), " +
                    " latitude DECIMAL(7, 5), " +
                    " longitude DECIMAL(7, 5), " +
                    " property_type VARCHAR(50), " +
                    " room_type VARCHAR(50), " +
                    " accommodates INTEGER, " +
                    " bathrooms INTEGER, " +
                    " bathrooms_text VARCHAR(50), " +
                    " bedrooms INTEGER, " +
                    " beds INTEGER, " +
                    " amenities VARCHAR(500), " +
                    " price VARCHAR(10), " +
                    " minimum_nights INTEGER, " +
                    " maximum_nights INTEGER, " +
                    " minimum_minimum_nights INTEGER, " +
                    " maximum_minimum_nights INTEGER, " +
                    " minimum_maximum_nights INTEGER, " +
                    " maximum_maximum_nights INTEGER, " +
                    " minimum_nights_avg_ntm INTEGER, " +
                    " maximum_nights_avg_ntm INTEGER, " +
                    " calendar_updated VARCHAR(1), " +
                    " has_availability VARCHAR(1), " +
                    " availability_30 INTEGER, " +
                    " availability_60 INTEGER, " +
                    " availability_90 INTEGER, " +
                    " availability_365 INTEGER, " +
                    " calendar_last_scraped DATE, " +
                    " number_of_reviews INTEGER, " +
                    " number_of_reviews_ltm INTEGER, " +
                    " number_of_reviews_l30d INTEGER, " +
                    " first_review DATE, " +
                    " last_review DATE, " +
                    " review_scores_rating DECIMAL(3, 2), " +
                    " review_scores_accuracy DECIMAL(3, 2), " +
                    " review_scores_cleanliness DECIMAL(3, 2), " +
                    " review_scores_checkin DECIMAL(3, 2), " +
                    " review_scores_communication DECIMAL(3, 2), " +
                    " review_scores_location DECIMAL(3, 2), " +
                    " review_scores_value DECIMAL(3, 2), " +
                    " license VARCHAR(1), " +
                    " instant_bookable VARCHAR(1), " +
                    " calculated_host_listings_count INTEGER, " +
                    " calculated_host_listings_count_entire_homes INTEGER, " +
                    " calculated_host_listings_count_private_rooms INTEGER, " +
                    " calculated_host_listings_count_shared_rooms INTEGER, " +
                    " reviews_per_month DECIMAL(3, 2), " +
                    " PRIMARY KEY (id))";
            statement.executeUpdate(createListing);
            System.out.println("Listing table created successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void dropListing(String[] args) {
        DB_URL = "jdbc:mysql://localhost/airbnb";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement statement = conn.createStatement()
        ) {
            String drop = "DROP TABLE listing";
            statement.executeUpdate(drop);
            System.out.println("Listing table dropped successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void addReview(String[] args) {
        DB_URL = "jdbc:mysql://localhost/airbnb";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement statement = conn.createStatement()
        ) {
            // add listing table
            String createReview = "CREATE TABLE review " +
                    "(listing_id INTEGER, " +
                    " id VARCHAR(100), " +
                    " date DATE, " +
                    " reviewer_id INTEGER, " +
                    " reviewer_name VARCHAR(20), " +
                    " comments VARCHAR(500), " +
                    " FOREIGN KEY (listing_id) REFERENCES listing(id) ON DELETE SET NULL)";
            statement.executeUpdate(createReview);
            System.out.println("Review table created successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void dropReview(String[] args) {
        DB_URL = "jdbc:mysql://localhost/airbnb";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement statement = conn.createStatement()
        ) {
            String drop = "DROP TABLE review";
            statement.executeUpdate(drop);
            System.out.println("Review table dropped successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void dropDatabase(String[] args) {
        DB_URL = "jdbc:mysql://localhost/";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement statement = conn.createStatement()
        ) {
            String drop = "DROP DATABASE airbnb";
            statement.executeUpdate(drop);
            System.out.println("Database dropped successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void populateListing(String[] args) {
        DB_URL = "jdbc:mysql://localhost/airbnb";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement statement = conn.createStatement()
        ) {
            // add listing table
            String populateListing = "LOAD DATA LOCAL INFILE '/Users/divyamgupta/Documents/georgia_tech/airbnb_etl/resources/listing.csv' " +
                    "INTO TABLE listing " +
                    "FIELDS TERMINATED BY ',' " +
                    "ENCLOSED BY '\"' " +
                    "LINES TERMINATED BY '\\n' " +
                    "IGNORE 1 ROWS";
            statement.executeUpdate(populateListing);
            System.out.println("Listing table populated successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void populateReview(String[] args) {
        DB_URL = "jdbc:mysql://localhost/airbnb";
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement statement = conn.createStatement()
        ) {
            // add listing table
            String populateReview = "LOAD DATA LOCAL INFILE '/Users/divyamgupta/Documents/georgia_tech/airbnb_etl/resources/review.csv' " +
                    "INTO TABLE review " +
                    "FIELDS TERMINATED BY ',' " +
                    "ENCLOSED BY '\"' " +
                    "LINES TERMINATED BY '\\n' " +
                    "IGNORE 1 ROWS";
            statement.executeUpdate(populateReview);
            System.out.println("Review table populated successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        createDatabase(args);
        addListing(args);
//        populateListing(args);
//        populateReview(args);
        addReview(args);
        dropReview(args);
        dropListing(args);
        dropDatabase(args);
    }

    // RUN QUERIES ON TERMINAL

        //    -- Order listing data by host's number of listings in descending order
        //    SELECT id, name, host_listings_count FROM listing
        //    ORDER BY host_listings_count DESC;
        //
        //-- Order listing data by number of beds in the accommodation in descending order
        //    SELECT id, name, beds FROM listing
        //    ORDER BY beds DESC;
        //
        //-- Give top 5 listings arranged by number of beds, bedrooms, and bathrooms in the accommodation in descending order
        //    SELECT id, name, beds, bedrooms, bathrooms FROM listing
        //    ORDER BY beds DESC, bedrooms DESC, bathrooms DESC
        //    LiMIT 5;
        //
        //-- Give the 5 most reviewed hosts with minimum beds
        //    SELECT id, name, number_of_reviews, beds FROM listing
        //    ORDER BY number_of_reviews DESC, bedrooms ASC
        //    LiMIT 5;
        //
        //-- Compute how many of the hosts came on-board AirBnb after 2007 who have more than 5 beds
        //    SELECT COUNT(id)
        //    FROM listing
        //    WHERE host_since > 2008-01-01 AND beds > 5;
        //
        //-- Compute the average of all the available accommodates in the listings
        //    SELECT AVG(accommodates)
        //    FROM listing
        //    WHERE has_availability = 'T';
        //
        //-- Give all those listings which have apartment type accommodations;
        //    SELECT id, name FROM listing
        //    WHERE name LIKE '% Apartment%';
        //
        //-- Give all the dates when each listing was reviewed
        //    SELECT listing.id, listing.name, listing.host_since, review.date
        //    FROM listing
        //    JOIN review
        //    ON listing.id = review.listing_id;
}