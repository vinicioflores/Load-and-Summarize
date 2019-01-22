CREATE DATABASE airbnb;

USE airbnb;

CREATE TABLE IF NOT EXISTS Calendar(listing_id BIGINT, date DATE, available VARCHAR(1), price STRING) 
PARTITIONED BY(city STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' STORED AS TEXTFILE
TBLPROPERTIES (
    'serialization.null.format' = '',
    'skip.header.line.count' = '1');


CREATE TABLE IF NOT EXISTS Listing(
id BIGINT
, listing_url STRING
, scrape_id BIGINT
, last_scraped DATE
, name STRING
, summary STRING
, space STRING
, description STRING 
, experiences_offered STRING
, neighborhood_overview STRING 
, notes STRING
, transit STRING
, access STRING 
, interaction STRING
, house_rules STRING 
, thumbnail_url STRING
, medium_url STRING
, picture_url STRING
, xl_picture_url STRING
, host_id BIGINT
, host_url STRING 
, host_name STRING
, host_since DATE
, host_location STRING
, host_about STRING
, host_response_time STRING
, host_response_rate STRING 
, host_acceptance_rate STRING
, host_is_superhost VARCHAR(1)
, host_thumbnail_url STRING
, host_picture_url STRING
, host_neighbourhood STRING
, host_listings_count INT
, host_total_listings_count INT
, host_verifications ARRAY<STRING>
, host_has_profile_pic VARCHAR(1)
, host_identity_verified VARCHAR(1)
, street STRING
, neighbourhood STRING
, neighbourhood_cleansed STRING
, neighbourhood_group_cleansed STRING 
, state STRING
, zipcode STRING
, market STRING
, smart_location STRING
, country_code  STRING
, country STRING 
, latitude DOUBLE
, longitude DOUBLE
, is_location_exact VARCHAR(1)
, property_type STRING
, room_type STRING
, accommodates TINYINT
, bathrooms TINYINT
, bedrooms TINYINT
, beds TINYINT
, bed_type STRING
, amenities ARRAY<STRING>
, square_feet INT
, price DECIMAL
, weekly_price DECIMAL 
, monthly_price DECIMAL
, security_deposit DECIMAL
, cleaning_fee DECIMAL
, guests_included TINYINT
, extra_people DECIMAL 
, minimum_nights TINYINT
, maximum_nights TINYINT
, calendar_updated STRING
, has_availability VARCHAR(1)
, availability_30 SMALLINT
, availability_60 SMALLINT
, availability_90 SMALLINT
, availability_365 SMALLINT
, calendar_last_scraped DATE
, number_of_reviews INT
, first_review DATE
, last_review DATE
, review_scores_rating SMALLINT
, review_scores_accuracy TINYINT
, review_scores_cleanliness TINYINT
, review_scores_checkin TINYINT
, review_scores_communication TINYINT
, review_scores_location TINYINT
, review_scores_value TINYINT 
, requires_license  VARCHAR(1)
, license STRING
, jurisdiction_names STRING
, instant_bookable VARCHAR(1)
, is_business_travel_ready VARCHAR(1)
, cancellation_policy STRING
, require_guest_profile_picture VARCHAR(1)
, require_guest_phone_verification VARCHAR(1)
, calculated_host_listings_count SMALLINT
, reviews_per_month DECIMAL
) 
PARTITIONED BY(city STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
TBLPROPERTIES (
    'serialization.null.format' = '',
    'skip.header.line.count' = '1');


LOAD DATA  INPATH '/user/root/calendar_London.csv' INTO TABLE Calendar PARTITION(city='London');
LOAD DATA  INPATH '/user/root/calendar_Hong_Kong.csv' INTO TABLE Calendar PARTITION(city='Hong Kong');
LOAD DATA  INPATH '/user/root/calendar_Santa_Clara.csv' INTO TABLE Calendar PARTITION(city='Santa Clara');

LOAD DATA  INPATH '/user/root/listings_Hong_Kong_NoCR.csv' INTO TABLE Listing PARTITION(city='Hong Kong');
LOAD DATA  INPATH '/user/root/listings_London_NoCR.csv' INTO TABLE Listing PARTITION(city='London');
LOAD DATA  INPATH '/user/root/listings_Santa_Clara_NoCR.csv' INTO TABLE Listing PARTITION(city='Santa Clara');


CREATE TABLE ListingBMV 
   STORED AS PARQUET AS
   SELECT * FROM Listing;

create view if not exists v_calendarLondon
as  SELECT * FROM calendar where  city='London' and price is not null ;

create view if not exists v_calendarHongKong
as  SELECT * FROM calendar where  city='Hong Kong' and price is not null;

create view if not exists v_calendarSantaClara
as  SELECT * FROM calendar where  city='Santa Clara' and price is not null;


create view if not exists v_listings_consumption
as SELECT id  AS id 
, CAST(listing_url AS VARCHAR(8000))  AS listing_url 
, scrape_id  AS scrape_id 
, last_scraped  AS last_scraped 
, CAST(name AS VARCHAR(8000))  AS name 
, CAST(summary AS VARCHAR(8000))  AS summary 
, CAST(space AS VARCHAR(8000))  AS space 
, CAST(description AS VARCHAR(8000))   AS description  
, CAST(experiences_offered AS VARCHAR(8000))  AS experiences_offered 
, CAST(neighborhood_overview AS VARCHAR(8000))   AS neighborhood_overview  
, CAST(notes AS VARCHAR(8000))  AS notes 
, CAST(transit AS VARCHAR(8000))  AS transit 
, CAST(access AS VARCHAR(8000))   AS access  
, CAST(interaction AS VARCHAR(8000))  AS interaction 
, CAST(house_rules AS VARCHAR(8000))   AS house_rules  
, CAST(thumbnail_url AS VARCHAR(8000))  AS thumbnail_url 
, CAST(medium_url AS VARCHAR(8000))  AS medium_url 
, CAST(picture_url AS VARCHAR(8000))  AS picture_url 
, CAST(xl_picture_url AS VARCHAR(8000))  AS xl_picture_url 
, host_id  AS host_id 
, CAST(host_url AS VARCHAR(8000))   AS host_url  
, CAST(host_name AS VARCHAR(8000))  AS host_name 
, host_since  AS host_since 
, CAST(host_location AS VARCHAR(8000))  AS host_location 
, CAST(host_about AS VARCHAR(8000))  AS host_about 
, CAST(host_response_time AS VARCHAR(8000))  AS host_response_time 
, CAST(host_response_rate AS VARCHAR(8000))   AS host_response_rate  
, CAST(host_acceptance_rate AS VARCHAR(8000))  AS host_acceptance_rate 
, host_is_superhost  AS host_is_superhost 
, CAST(host_thumbnail_url AS VARCHAR(8000))  AS host_thumbnail_url 
, CAST(host_picture_url AS VARCHAR(8000))  AS host_picture_url 
, CAST(host_neighbourhood AS VARCHAR(8000))  AS host_neighbourhood 
, host_listings_count  AS host_listings_count 
, host_total_listings_count  AS host_total_listings_count 
, host_verifications  AS host_verifications 
, host_has_profile_pic  AS host_has_profile_pic 
, host_identity_verified  AS host_identity_verified 
, CAST(street AS VARCHAR(8000))  AS street 
, CAST(neighbourhood AS VARCHAR(8000))  AS neighbourhood 
, CAST(neighbourhood_cleansed AS VARCHAR(8000))  AS neighbourhood_cleansed 
, CAST(neighbourhood_group_cleansed AS VARCHAR(8000))   AS neighbourhood_group_cleansed  
, CAST(state AS VARCHAR(8000))  AS state 
, CAST(zipcode AS VARCHAR(8000))  AS zipcode 
, CAST(market AS VARCHAR(8000))  AS market 
, CAST(smart_location AS VARCHAR(8000))  AS smart_location 
, CAST(country_code AS VARCHAR(8000))   AS country_code  
, CAST(country AS VARCHAR(8000))   AS country  
, latitude  AS latitude 
, longitude  AS longitude 
, is_location_exact  AS is_location_exact 
, CAST(property_type AS VARCHAR(8000))  AS property_type 
, CAST(room_type AS VARCHAR(8000))  AS room_type 
, accommodates  AS accommodates 
, bathrooms  AS bathrooms 
, bedrooms  AS bedrooms 
, beds  AS beds 
, CAST(amenities AS VARCHAR(8000))  AS bed_type 
, CAST(concat_ws(",", square_feet) AS VARCHAR(8000))  AS amenities 
, CAST(bed_type AS VARCHAR(8000))  AS square_feet 
, price  AS price 
, weekly_price   AS weekly_price  
, monthly_price  AS monthly_price 
, security_deposit  AS security_deposit 
, cleaning_fee  AS cleaning_fee 
, guests_included  AS guests_included 
, extra_people   AS extra_people  
, minimum_nights  AS minimum_nights 
, maximum_nights  AS maximum_nights 
, CAST(calendar_updated AS VARCHAR(8000))  AS calendar_updated 
, has_availability  AS has_availability 
, availability_30  AS availability_30 
, availability_60  AS availability_60 
, availability_90  AS availability_90 
, availability_365  AS availability_365 
, calendar_last_scraped  AS calendar_last_scraped 
, number_of_reviews  AS number_of_reviews 
, first_review  AS first_review 
, last_review  AS last_review 
, review_scores_rating  AS review_scores_rating 
, review_scores_accuracy  AS review_scores_accuracy 
, review_scores_cleanliness  AS review_scores_cleanliness 
, review_scores_checkin  AS review_scores_checkin 
, review_scores_communication  AS review_scores_communication 
, review_scores_location  AS review_scores_location 
, review_scores_value   AS review_scores_value  
, requires_license   AS requires_license  
, CAST(license AS VARCHAR(8000))  AS license 
, CAST(instant_bookable AS VARCHAR(8000))  AS jurisdiction_names 
, is_business_travel_ready  AS instant_bookable 
, is_business_travel_ready  AS is_business_travel_ready 
, CAST(cancellation_policy AS VARCHAR(8000))  AS cancellation_policy 
, require_guest_profile_picture  AS require_guest_profile_picture 
, require_guest_phone_verification  AS require_guest_phone_verification 
, calculated_host_listings_count  AS calculated_host_listings_count 
, reviews_per_month  AS reviews_per_month
,city as city FROM listing where  neighbourhood is not null and neighbourhood != ''
and review_scores_cleanliness is not null and review_scores_cleanliness != ''
and review_scores_rating is not null and review_scores_rating != ''
and review_scores_accuracy is not null and review_scores_accuracy != ''
and review_scores_checkin is not null and review_scores_checkin != ''
and review_scores_communication is not null and review_scores_communication != ''
and review_scores_location is not null and review_scores_location != ''
and review_scores_value is not null and review_scores_value != ''
;

/*
	{TV,"Cable TV",Internet,Wifi,"Air conditioning",Kitchen,Heating,"Family/kid friendly",Washer,Essentials,Shampoo}
*/
create view if not exists v_amenities
as select  id as listing_id, get_json_object(amenities) as amenity from v_listings_consumption;



create view if not exists v_listingsLondon
as  select * FROM v_listings_consumption where  city='London';

create view if not exists v_listingsHongKong
as  SELECT * FROM v_listings_consumption where  city='Hong Kong';

create view if not exists v_listingsSantaClara
as  SELECT * FROM v_listings_consumption where  city='Santa Clara';

select  
  explode(split(translate(translate(amenities, '{', ''), '}', ''), ',')) as amenity
from v_listings_consumption;