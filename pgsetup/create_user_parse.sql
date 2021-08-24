CREATE SCHEMA retail;

CREATE TABLE retail.user_parse (
    zone varchar(10),
    bossencounter varchar(20),
    characterid varchar(1000),
    parse int,
    date timestamp,
    dps Numeric(8,3),
    uniqueid int,
    server varchar(20)
);

COPY retail.user_parse(zone,bossencounter,character_id,parse,date,dps,uniqueid,server) 
FROM '/input_data/ParseData.csv' DELIMITER ','  CSV HEADER;
