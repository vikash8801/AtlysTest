CREATE TABLE HistoricalData (
    Date DATE,
    Open FLOAT,
    High FLOAT,
    Low FLOAT,
    Close FLOAT,
    Volume INT,
    Company VARCHAR(30),
);

CREATE INDEX company_date_index ON HistoricalData (Company,Date);
CREATE INDEX company_volume_index ON HistoricalData (Company,Volume);
CREATE INDEX company_close_index ON HistoricalData (Company,Close);
CREATE INDEX company_open_index ON HistoricalData (Company,Open);


--Company wise daily volume change
select Company, Date, Volume from HistoricalData order by Company, Date;

--Company wise daily variation of price
select company, Date, (Close-Open) as Variation from HistoricalData order by Company, Date;


WITH variations AS (
    SELECT company, Date, (Close-Open) as Variation from HistoricalData
)
SELECT company, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Variation) as Median_Variation
FROM variations
GROUP BY company;
