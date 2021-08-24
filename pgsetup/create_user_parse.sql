CREATE SCHEMA retail;

CREATE TABLE retail.user_parse (
    Zone VARCHAR(10),
    BossEncounter VARCHAR(20),
    CharacterID VARCHAR(50),
    Parse INTEGER,
    Date TIMESTAMP,
    DPS DECIMAL(8, 3),
    UniqueID VARCHAR(30),
    Server VARCHAR(20)
);

COPY retail.user_parse(zone,bossencounter,character_id,parse,date,dps,uniqueid,server) 
FROM '/input_data/ParseData.csv' DELIMITER ','  CSV HEADER;
