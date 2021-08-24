COPY (
       select Zone,
              BossEncounter,
              CharacterID,
              Parse,
              Date,
              DPS,
              UniqueID,
              Server
       from retail.user_parse -- could have a date filter here to pull only required data
) TO '{{ params.user_parse }}' WITH (FORMAT CSV, HEADER);
