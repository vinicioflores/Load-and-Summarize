/**  Count of listings per city classified accross 5 price ranges/levels  **/

CREATE TABLE ListingsByPriceRangeAvro(PriceRange STRING, City STRING, ListingCount BIGINT) STORED AS AVRO;
CREATE TABLE ListingsByPriceRange(PriceRange STRING, City STRING, ListingCount BIGINT);



INSERT INTO ListingsByPriceRange
SELECT 

  case 
     when (zeroifnull(CAST(price as double)) <= 90.0) then 'Very Cheap'  
     when (zeroifnull(CAST(price as double)) <= 145.0 and zeroifnull(CAST(price as double)) > 90.0) then 'Cheap' 
     when (zeroifnull(CAST(price as double)) <= 205.0 and zeroifnull(CAST(price as double)) > 145.0) then 'Regular'
     when (zeroifnull(CAST(price as double)) <= 285.0 and zeroifnull(CAST(price as double)) > 205.0) then 'Expensive'
     else 'Very Expensive' end as PriceRange  
     , City
     ,  COUNT(*)
FROM airbnb.ListingBMV
GROUP BY City,PriceRange
ORDER BY City,PriceRange;

select * from ListingsByPriceRange;


/***** FOR ETL CONSUMPTION ONLY - RUN IN HIVE *****/

insert into ListingsByPriceRangeAvro
select * from ListingsByPriceRange;


select *
from
       ListingsByPriceRangeAvro
;
 


/** Assuming 3 levels of cleanliness: 1) Excelent 2) Moderate 3) Not very clean. Group listings by city by cleanliness level. **/


CREATE TABLE ListingsByCleanlinessAvro(Category STRING, City STRING, ListingCount BIGINT)  STORED AS AVRO;
CREATE TABLE ListingsByCleanliness(Category STRING, City STRING, ListingCount BIGINT);

INSERT INTO ListingsByCleanliness
SELECT 

  case 
     when (zeroifnull(CAST(review_scores_cleanliness as double)) <= 3) then 'Not very clean'  
     when (zeroifnull(CAST(review_scores_cleanliness as double)) <= 6 and zeroifnull(CAST(review_scores_cleanliness as double)) > 3) then 'Moderate' 
     else 'Excellent' end as Cleanliness
     , City
     ,  COUNT(*)
FROM airbnb.ListingBMV
GROUP BY City,Cleanliness
ORDER BY City,Cleanliness;

select * from ListingsByCleanliness;



/***** FOR ETL CONSUMPTION ONLY - RUN IN HIVE *****/

insert into ListingsByCleanlinessAvro
select * from ListingsByCleanliness;


select *
from
       ListingsByCleanlinessAvro
;
 