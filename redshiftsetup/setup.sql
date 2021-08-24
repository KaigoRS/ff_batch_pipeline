-- This is run as part of the setup for the redshift spectrum
CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG DATABASE 'spectrumdb' iam_role 'arn:aws:iam::"$AWS_ID":role/"$IAM_ROLE_NAME"' CREATE EXTERNAL DATABASE IF NOT EXISTS;
DROP TABLE IF EXISTS spectrum.user_parse_staging;
CREATE EXTERNAL TABLE spectrum.user_parse_staging (
    Zone VARCHAR(10),
    BossEncounter VARCHAR(20),
    CharacterID VARCHAR(50),
    Parse INTEGER,
    Date TIMESTAMP,
    DPS DECIMAL(8, 3),
    UniqueID VARCHAR(30)
    Server VARCHAR(20)
) PARTITIONED BY (insert_date DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS textfile LOCATION 's3://"$1"/stage/user_parse/' TABLE PROPERTIES ('skip.header.line.count' = '1');
DROP TABLE IF EXISTS spectrum.classified_party_review;
CREATE EXTERNAL TABLE spectrum.classified_party_review (
    ReportID VARCHAR(30),
    PartyStatus boolean,
    InsertDate VARCHAR(12)
) STORED AS PARQUET LOCATION 's3://"$1"/stage/Party_Information/';
DROP TABLE IF EXISTS public.user_parse_metric;
CREATE TABLE public.user_parse_metric (
        ReportID VARCHAR(50),
        PartyDps DECIMAL(10, 3),
        PartyScore INT,
        Spot_1 VARCHAR(20),
        Spot_2 VARCHAR(20),
        Spot_3 VARCHAR(20),
        Spot_4 VARCHAR(20),
        Spot_5 VARCHAR(20),
        Spot_6 VARCHAR(20),
        Spot_7 VARCHAR(20),
        Spot_8 VARCHAR(20),
        InsertDate TIMESTAMP
    );
