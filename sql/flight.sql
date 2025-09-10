USE flightdb;

-- join daily weather by date 
CREATE OR REPLACE VIEW flights_with_weather AS
SELECT f.*,
       w.prcp, w.tmax, w.tmin, w.awnd, w.wsf2
FROM flights_raw f
LEFT JOIN weather_daily w
  ON w.date = f.flight_date;

-- monthly route
CREATE OR REPLACE VIEW route_summary AS
SELECT
  carrier_code,
  destination_airport,
  DATE_FORMAT(flight_date, '%Y-%m') AS yyyymm,
  COUNT(*)                                   AS flights,
  AVG(dep_delay_minutes)                     AS avg_dep_delay,
  AVG(delay_weather_minutes)                 AS avg_wx_delay,
  SUM(CASE WHEN dep_delay_minutes > 15 THEN 1 ELSE 0 END) / COUNT(*) AS pct_delayed,
  0.4*AVG(dep_delay_minutes) + 10*AVG(CASE WHEN IFNULL(delay_weather_minutes,0) > 0 THEN 1 ELSE 0 END) AS risk_score
FROM flights_raw
GROUP BY 1,2,3;

-- top 10 riskiest routes
SELECT * FROM route_summary
ORDER BY risk_score DESC
LIMIT 10;

-- delay trend by month
SELECT yyyymm, ROUND(AVG(avg_dep_delay),1) AS avg_delay
FROM route_summary
GROUP BY yyyymm
ORDER BY yyyymm;

-- worse destination by departure delay
SELECT destination_airport, ROUND(AVG(dep_delay_minutes),1) AS avg_dep_delay
FROM flights_raw
GROUP BY destination_airport
ORDER BY avg_dep_delay DESC
LIMIT 10;
