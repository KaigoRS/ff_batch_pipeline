DELETE FROM public.user_parse_metric
WHERE insert_date = '{{ ds }}';
INSERT INTO public.user_parse_metric (
        ReportID,
        PartyDps,
        PartyScore,
        Spot_1,
        Spot_2,
        Spot_3,
        Spot_4,
        Spot_5,
        Spot_6,
        Spot_7,
        Spot_8,
        InsertDate
    )
SELECT ups.ReportID,
    CAST(SUM(ups.DPS) AS DECIMAL(10, 3)) AS PartyDps,
	SUM(ups.Parse) AS PartyScore,
       p1.ClassID as Spot_1,
       p2.ClassID as Spot_2,
       p3.ClassID as Spot_3,
       p4.ClassID as Spot_4,
       p5.ClassID as Spot_5,
       p6.ClassID as Spot_6,
       p7.ClassID as Spot_7,
       p8.ClassID as Spot_8,
       '{{ ds }}' as insert_date

FROM spectrum.user_parse_staging ups
LEFT JOIN spectrum.classified_party_review p1 ON Spot_1 = p1.PartySpot
LEFT JOIN spectrum.classified_party_review p2 ON Spot_2 = p2.PartySpot
LEFT JOIN spectrum.classified_party_review p3 ON Spot_3 = p3.PartySpot
LEFT JOIN spectrum.classified_party_review p4 ON Spot_4 = p4.PartySpot
LEFT JOIN spectrum.classified_party_review p5 ON Spot_5 = p5.PartySpot
LEFT JOIN spectrum.classified_party_review p6 ON Spot_6 = p6.PartySpot
LEFT JOIN spectrum.classified_party_review p7 ON Spot_7 = p7.PartySpot
LEFT JOIN spectrum.classified_party_review p8 ON Spot_8 = p8.PartySpot

GROUP BY ups.ReportID;
