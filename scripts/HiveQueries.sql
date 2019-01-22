/*** Top 3 more experienced hosts per city ***/

CREATE TABLE Top3MoreExperiencedHostsPerCityAvro
             (
                          host_name STRING
                        , host_id BIGINT
                        , city STRING
                        , RankingPosition INT
                        , ListingCount    BIGINT
             )
STORED AS AVRO
;




INSERT INTO Top3MoreExperiencedHostsPerCityAvro
select *
from
       (
                    SELECT
                                 host_name
                               , host_id
                               , city
                               , RANK() OVER (
                                 PARTITION BY City
                                 ORDER BY
                                              COUNT(DISTINCT id) OVER (
                                                          PARTITION BY City
                                                                     , host_id) DESC ) as RankingPosition
                               , COUNT(DISTINCT id) OVER (
                                             PARTITION BY City
                                                        , host_id) as ListingCount
                    FROM
                                 airbnb.Listing
                    GROUP BY
                                 host_name
                               , host_id
                               , city
                    ORDER BY
                                 ListingCount DESC
       )
       tbl
WHERE
       tbl.RankingPosition <= 3
;

select *
from
       Top3MoreExperiencedHostsPerCityAvro
;

/**** The  cities  sorted descending by experience being airbnb hosts. Amount of years + amount of people using it **/


CREATE TABLE CitiesOrderedByHostingExperienceMeasuresAvro
             (
                          city STRING
                        , OldestHostRegistration DATE
                        , TotalHostsCount        bigint
             )
stored as AVRO
;

insert into CitiesOrderedByHostingExperienceMeasuresAvro

select
         city
       , min(distinct from_unixtime(unix_timestamp(host_since,'MM/dd/yyyy'),'yyyy-MM-dd')) as OldestHostRegistration
       , count(distinct host_id)                                                           AS TotalHostsCount
from
         airbnb.Listing
where
         from_unixtime(unix_timestamp(host_since,'MM/dd/yyyy'),'yyyy-MM-dd') is not null
group by
         city
order by
         TotalHostsCount
       , OldestHostRegistration DESC
;


select *
from
       CitiesOrderedByHostingExperienceMeasuresAvro
;
 


 --------- TEST 1 --------------
 
select city, SeniorHostCount, MinHostSince
from
         (
                      select
                                   city
                                 , RankingPositionCount
                                 , RankingPositionTime
                                 , MinHostSince
                                 , COUNT(DISTINCT host_id) as SeniorHostCount
                      from
                                   (
                                                select
                                                             city
                                                           , host_id
                                                           , host_since
                                                           , RANK() OVER (
                                                             PARTITION BY City
                                                             ORDER BY
                                                                          COUNT(DISTINCT host_id) OVER (
                                                                                           PARTITION BY City) DESC ) as RankingPositionCount
                                                           , RANK() OVER (
                                                             PARTITION BY City
                                                             ORDER BY
                                                                          MIN(DISTINCT host_since) OVER (
                                                                                            PARTITION BY City) DESC ) as RankingPositionTime
                                                           , COUNT(DISTINCT host_id) OVER (
                                                                              PARTITION BY City
                                                                                         , host_id) as TotalHostCount
                                                           , MIN(DISTINCT host_since) OVER(
                                                                              PARTITION BY City
                                                                                         , host_id) as MinHostSince
                                                
                                                FROM
                                                             airbnb.Listing
                                                where
                                                             from_unixtime(unix_timestamp(host_since,'MM/dd/yyyy'),'yyyy-MM-dd') is not null
                                                
                                                group by
                                                             city
                                                           , host_id
                                                           , host_since
                                   )
                                   cities
                      
                    where
                                  year(from_unixtime(unix_timestamp(host_since,'MM/dd/yyyy'),'yyyy-MM-dd')) == year(from_unixtime(unix_timestamp(MinHostSince,'MM/dd/yyyy'), 'yyyy-MM-dd'))
                      
                      
                      group by
                                   city
                                 , RankingPositionCount
                                 , RankingPositionTime
                                 , MinHostSince
         )
         agg1
order by
         SeniorHostCount
       , MinHostSince DESC
;
/*** Top 7 most common amenities per city ***/

CREATE TABLE Top7AmenitiesPerCityAvro
             (
                          amenities STRING
                        , city STRING
                        , RankingPosition INT
                        , AmenitiesCount  BIGINT
             ) STORED AS AVRO
;

insert into Top7AmenitiesPerCityAvro
select *
from
       (
                    SELECT
                                 amenities
                               , city
                               , RANK() OVER (
                                 PARTITION BY City
                                 ORDER BY
                                              COUNT(DISTINCT id) OVER (
                                                          PARTITION BY City
                                                                     , Amenities) DESC ) as RankingPosition
                               , COUNT(DISTINCT id) OVER (
                                             PARTITION BY City
                                                        , Amenities) as AmenitiesCount
                    FROM
                                 airbnb.listing
                    WHERE
                                 amenities IS NOT NULL
                                 and amenities      <> '...'
                    GROUP BY
                                 amenities
                               , city
                    ORDER BY
                                 City
                               , AmenitiesCount DESC
       )
       tbl
WHERE
       tbl.RankingPosition <= 7
;

select *
from
       Top7AmenitiesPerCityAvro
;




/***********    TO GET THE BOUNDS OF CALENDAR BOOKINGS - TO PULL ONLY A SMALL SUBSET OF WEATHER DATA *********/
select min(date) as begindate, max(date) as enddate from Calendar
join listing on listing.id = Calendar.listing_id
group by city

