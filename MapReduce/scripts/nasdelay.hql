DROP TABLE IF EXISTS delayed_flights;

CREATE EXTERNAL TABLE delayed_flights (
		Year	int,
        Month	int,
        DayofMonth	int,
        DayOfWeek	int,
        DepTime	int,
        CRSDepTime	int,
        ArrTime	int,
        CRSArrTime	int,
        UniqueCarrier	string,
        FlightNum	int,
        TailNum	string,
        ActualElapsedTime	int,
        CRSElapsedTime	int,
        AirTime	double,
        ArrDelay	int,
        DepDelay	int,
        Origin	string,
        Dest	string,
        Distance	int,
        TaxiIn	int,
        TaxiOut	int,
        Cancelled	int,
        CancellationCode	string,
        Diverted	int,
        CarrierDelay	int,
        WeatherDelay	int,
        NASDelay	int,
        SecurityDelay	int,
        LateAircraftDelay	int
	)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY '\n'
	STORED AS TEXTFILE
	LOCATION "${INPUT}";

 INSERT OVERWRITE DIRECTORY "${OUTPUT}"
 SELECT Year, avg((NASDelay/ArrDelay)*100) FROM delayed_flights GROUP BY Year;