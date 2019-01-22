CREATE DATABASE
IF NOT EXISTS datamart;

USE datamart;

Drop View
If Exists v_CalendarBookings;
Drop View
If Exists v_Listings;
Drop View
If Exists v_WeatherForecasts;
Drop View
If Exists v_ListingsByCleanliness;
Drop View
If Exists v_ListingsByPriceRange;
Drop View
If Exists v_CitiesByHostingExperience;

Drop Table
If Exists tblCitiesOrderedByHostingExperienceMeasures;
Drop Table
If Exists tblListingsByCleanliness;
Drop Table
If Exists tblListingsByPriceRange;

Drop Table
If Exists tblStagingCalendarErrors;
Drop Table
If Exists tblStagingCitiesOrderedByHostingExperienceMeasuresAvroErrors;
Drop Table
If Exists tblStagingListingsByCleanlinessErrors;
Drop Table
If Exists tblStagingListingsByPriceRangeErrors;

Drop Table
If Exists tblCalendar;
Drop Table
If Exists tblCalendarErrors;

Drop Table
If Exists tblWeatherForecast;
Drop Table
If Exists tblWeatherForecastErrors;

Drop Table
If Exists tblListings;

Drop Table
If Exists tblListingsErrors

CREATE TABLE
IF NOT EXISTS tblCitiesOrderedByHostingExperienceMeasures ( City VARCHAR(50) , OldestHostRegistration DATE , TotalHostsCount BIGINT );

CREATE TABLE
IF NOT EXISTS tblListingsByCleanliness ( Category VARCHAR(100) , City VARCHAR(50) , ListingCount BIGINT );

CREATE TABLE
IF NOT EXISTS tblListingsByPriceRange ( PriceRange VARCHAR(100) , City VARCHAR(50) , ListingCount BIGINT );

CREATE TABLE
IF NOT EXISTS tblStagingCalendarErrors ( error_code INT , error_column VARCHAR(255) , listing_id VARCHAR(8000) , city VARCHAR(8000) , calendar_date VARCHAR(8000) , available VARCHAR(8000) , price VARCHAR(8000) );

CREATE TABLE
IF NOT EXISTS tblStagingCitiesOrderedByHostingExperienceMeasuresAvroErrors ( error_code INT , error_column VARCHAR(255) , City VARCHAR(8000) , OldestHostRegistration VARCHAR(8000) , TotalHostsCount VARCHAR(8000) );

CREATE TABLE
IF NOT EXISTS tblStagingListingsByCleanlinessErrors ( error_code INT , error_column VARCHAR(255) , Category VARCHAR(8000) , City VARCHAR(8000) , ListingCount VARCHAR(8000) );

CREATE TABLE
IF NOT EXISTS tblStagingListingsByPriceRangeErrors ( error_code INT , error_column VARCHAR(255) , PriceRange VARCHAR(8000) , City VARCHAR(8000) , ListingCount VARCHAR(8000) );

CREATE TABLE
IF NOT EXISTS tblCalendar ( listing_id BIGINT , city VARCHAR(50) , calendar_date DATE , available BIT , price DECIMAL(7,3) );

CREATE TABLE
IF NOT EXISTS tblCalendarErrors ( error_code INT , error_column VARCHAR(255) , listing_id VARCHAR(8000) , city VARCHAR(8000) , calendar_date VARCHAR(8000) , available VARCHAR(8000) , price VARCHAR(8000) );

CREATE TABLE
IF NOT EXISTS tblWeatherForecast ( dataDate DATETIME , type VARCHAR(50) , Country VARCHAR(50) , City VARCHAR(50) , Latitude DOUBLE , Longitude DOUBLE , ForecastDate DATE , WindDirection VARCHAR(50) , FeelsLikeTemperature TINYINT , WindGust TINYINT , ScreenRelativeHumidity TINYINT , PrecipitationProbability TINYINT , WindSpeed TINYINT , Temperature TINYINT , Visibility VARCHAR(50) , WeatherType TINYINT , MaxUVIndex TINYINT );

CREATE TABLE
IF NOT EXISTS tblWeatherForecastErrors ( error_code INT , error_column VARCHAR(255) , dataDate VARCHAR(8000) , type VARCHAR(8000) , Country VARCHAR(8000) , City VARCHAR(8000) , Latitude VARCHAR(8000) , Longitude VARCHAR(8000) , ForecastDate VARCHAR(8000) , WindDirection VARCHAR(8000) , FeelsLikeTemperature VARCHAR(8000) , WindGust VARCHAR(8000) , ScreenRelativeHumidity VARCHAR(8000) , PrecipitationProbability VARCHAR(8000) , WindSpeed VARCHAR(8000) , Temperature VARCHAR(8000) , Visibility VARCHAR(8000) , WeatherType VARCHAR(8000) , MaxUVIndex VARCHAR(8000) );

CREATE TABLE
IF NOT EXISTS tblListings ( id BIGINT NOT NULL PRIMARY KEY , listing_url varchar(1000) , scrape_id BIGINT , last_scraped DATE , name VARCHAR(8000) , summary VARCHAR(8000) , space VARCHAR(8000) , description VARCHAR(8000) , experiences_offered VARCHAR(8000) , neighborhood_overview VARCHAR(8000) , notes VARCHAR(8000) , transit VARCHAR(8000) , access VARCHAR(8000) , interaction VARCHAR(8000) , house_rules VARCHAR(8000) , thumbnail_url varchar(1000) , medium_url varchar(1000) , picture_url varchar(1000) , xl_picture_url varchar(1000) , host_id BIGINT , host_url varchar(1000) , host_name varchar(1000) , host_since DATE , host_location varchar(1000) , host_about varchar(1000) , host_response_time varchar(8000) , host_response_rate varchar(8000) , host_acceptance_rate varchar(8000) , host_is_superhost BIT , host_thumbnail_url varchar(1000) , host_picture_url varchar(1000) , host_neighbourhood varchar(1000) , host_listings_count INT , host_total_listings_count INT , host_verifications VARCHAR(8000) , host_has_profile_pic BIT , host_identity_verified BIT , street varchar(8000) , neighbourhood varchar(8000) , neighbourhood_cleansed varchar(8000) , neighbourhood_group_cleansed varchar(8000) , state varchar(1000) , zipcode varchar(1000) , market varchar(1000) , smart_location varchar(1000) , country_code varchar(1000) , country varchar(1000) , latitude DOUBLE , longitude DOUBLE , is_location_exact BIT , property_type varchar(1000) , room_type varchar(1000) , accommodates TINYINT , bathrooms TINYINT , bedrooms TINYINT , beds TINYINT , bed_type varchar(1000) , amenities VARCHAR(8000) , square_feet INT , price DECIMAL , weekly_price DECIMAL , monthly_price DECIMAL , security_deposit DECIMAL , cleaning_fee DECIMAL , guests_included TINYINT , extra_people DECIMAL , minimum_nights TINYINT , maximum_nights TINYINT , calendar_updated varchar(1000) , has_availability BIT , availability_30 SMALLINT , availability_60 SMALLINT , availability_90 SMALLINT , availability_365 SMALLINT , calendar_last_scraped DATE , number_of_reviews INT , first_review DATE , last_review DATE , review_scores_rating SMALLINT , review_scores_accuracy TINYINT , review_scores_cleanliness TINYINT , review_scores_checkin TINYINT , review_scores_communication TINYINT , review_scores_location TINYINT , review_scores_value TINYINT , requires_license BIT , license varchar(1000) , jurisdiction_names varchar(1000) , instant_bookable BIT , is_business_travel_ready BIT , cancellation_policy varchar(1000) , require_guest_profile_picture BIT , require_guest_phone_verification BIT , calculated_host_listings_count SMALLINT , reviews_per_month DECIMAL(6,3) , city VARCHAR(255) );

CREATE TABLE 
IF NOT EXISTS tblListingsErrors (error_code INT , error_column VARCHAR(255) , id varchar(8000) , listing_url varchar(8000) , scrape_id BIGINT , last_scraped DATE , name VARCHAR(8000) , summary VARCHAR(8000) , space VARCHAR(8000) , description VARCHAR(8000) , experiences_offered VARCHAR(8000) , neighborhood_overview VARCHAR(8000) , notes VARCHAR(8000) , transit VARCHAR(8000) , access VARCHAR(8000) , interaction VARCHAR(8000) , house_rules VARCHAR(8000) , thumbnail_url varchar(8000) , medium_url varchar(8000) , picture_url varchar(8000) , xl_picture_url varchar(8000) , host_id BIGINT , host_url varchar(8000) , host_name varchar(8000) , host_since DATE , host_location varchar(8000) , host_about varchar(8000) , host_response_time varchar(8000) , host_response_rate varchar(8000) , host_acceptance_rate varchar(8000) , host_is_superhost varchar(8000) , host_thumbnail_url varchar(8000) , host_picture_url varchar(8000) , host_neighbourhood varchar(8000) , host_listings_count INT , host_total_listings_count INT , host_verifications VARCHAR(8000) , host_has_profile_pic varchar(8000) , host_identity_verified varchar(8000) , street varchar(8000) , neighbourhood varchar(8000) , neighbourhood_cleansed varchar(8000) , neighbourhood_group_cleansed varchar(8000) , state varchar(8000) , zipcode varchar(8000) , market varchar(8000) , smart_location varchar(8000) , country_code varchar(8000) , country varchar(8000) , latitude DOUBLE , longitude DOUBLE , is_location_exact varchar(8000) , property_type varchar(8000) , room_type varchar(8000) , accommodates varchar(8000) , bathrooms varchar(8000) , bedrooms varchar(8000) , beds varchar(8000) , bed_type varchar(8000) , amenities VARCHAR(8000) , square_feet INT , price varchar(8000) , weekly_price varchar(8000) , monthly_price varchar(8000) , security_deposit varchar(8000) , cleaning_fee varchar(8000) , guests_included varchar(8000) , extra_people varchar(8000) , minimum_nights varchar(8000) , maximum_nights varchar(8000) , calendar_updated varchar(8000) , has_availability varchar(8000) , availability_30 varchar(8000) , availability_60 varchar(8000) , availability_90 varchar(8000) , availability_365 varchar(8000) , calendar_last_scraped DATE , number_of_reviews INT , first_review DATE , last_review DATE , review_scores_rating varchar(8000) , review_scores_accuracy varchar(8000) , review_scores_cleanliness varchar(8000) , review_scores_checkin varchar(8000) , review_scores_communication varchar(8000) , review_scores_location varchar(8000) , review_scores_value varchar(8000) , requires_license varchar(8000) , license varchar(8000) , jurisdiction_names varchar(8000) , instant_bookable varchar(8000) , is_business_travel_ready varchar(8000) , cancellation_policy varchar(8000) , require_guest_profile_picture varchar(8000) , require_guest_phone_verification varchar(8000) , calculated_host_listings_count varchar(8000) , reviews_per_month varchar(8000) , city VARCHAR(8000) );



Create View v_CalendarBookings As
Select *
From
       tblCalendar
;

Create View v_Listings As
Select *
From
       tblListings
;

Create View v_WeatherForecasts AS
Select *
From
       tblWeatherForecast
;

Create View v_ListingsByCleanliness AS
Select *
From
       tblListingsByCleanliness
;

Create View v_ListingsByPriceRange AS
Select *
From
       tblListingsByPriceRange
;

Create View v_CitiesByHostingExperience As
Select *
From
       tblCitiesOrderedByHostingExperienceMeasures
;