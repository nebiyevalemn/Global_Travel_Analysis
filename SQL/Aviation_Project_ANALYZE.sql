----1) Composite Tourism Performance Index and City Ranking Based on Hotels, Restaurants, and Attractions
create table city_tourism_performance_rank as
WITH HotelStats AS (
    SELECT
        city_key,
        MIN(city) AS city_name,
        ROUND(AVG(rating), 2) AS avg_htl,
        SUM(NVL(review_count, 0)) AS htl_revs
    FROM HOTELS
    GROUP BY city_key
),
RestStats AS (
    SELECT
        city_key,
        MIN(city) AS city_name,
        ROUND(AVG(rating), 2) AS avg_rst,
        SUM(NVL(review_count, 0)) AS rst_revs
    FROM RESTAURANTS
    GROUP BY city_key
),
AttrStats AS (
    SELECT
        city_key,
        MIN(city) AS city_name,
        ROUND(AVG(review_score), 2) AS avg_attr,
        SUM(NVL(review_count, 0)) AS attr_revs
    FROM ATTRACTIONS
    GROUP BY city_key
)
SELECT
    h.city_key,
    NVL(h.city_name, NVL(r.city_name, a.city_name)) AS city_name,
    h.avg_htl AS hotel_score,
    r.avg_rst AS restaurant_score,
    a.avg_attr AS attraction_score,
    h.htl_revs AS hotel_reviews,
    r.rst_revs AS restaurant_reviews,
    a.attr_revs AS attraction_reviews,
    (h.htl_revs + r.rst_revs + a.attr_revs) AS total_reviews,
    ROUND(
        (NVL(h.avg_htl, 0) * 0.4) +
        (NVL(r.avg_rst, 0) * 0.3) +
        (NVL(a.avg_attr, 0) * 0.3),
        2
    ) AS composite_index,
    RANK() OVER (
        ORDER BY
            (
                (NVL(h.avg_htl, 0) * 0.4) +
                (NVL(r.avg_rst, 0) * 0.3) +
                (NVL(a.avg_attr, 0) * 0.3)
            ) DESC
    ) AS city_rank
FROM HotelStats h
JOIN RestStats r
    ON h.city_key = r.city_key
JOIN AttrStats a
    ON h.city_key = a.city_key
WHERE h.htl_revs > 500
  AND r.rst_revs > 500
  AND a.attr_revs > 500
ORDER BY city_rank, total_reviews DESC;

-- 2)Best Value Cities: Hotel Quality vs Median Price Analysis
create table city_hotel_value_index as

WITH MedianCosts AS (
    SELECT DISTINCT 
           city_key,
           PERCENTILE_CONT(0.5) 
           WITHIN GROUP (ORDER BY price_final) 
           OVER (PARTITION BY city_key) AS median_hotel_price
    FROM HOTELS
),
QualityScores AS (
    SELECT 
        city_key, 
        AVG(NVL(rating,0)) AS avg_city_rating
    FROM HOTELS 
    GROUP BY city_key
)

SELECT 
    m.city_key,
    m.median_hotel_price,
    ROUND(q.avg_city_rating,2) AS avg_city_rating,
    ROUND(q.avg_city_rating / NULLIF(m.median_hotel_price, 0) * 100, 2) AS value_index,
    DENSE_RANK() OVER(
        ORDER BY (q.avg_city_rating / NULLIF(m.median_hotel_price, 0)) DESC
    ) AS value_rank
FROM MedianCosts m
JOIN QualityScores q 
    ON m.city_key = q.city_key
WHERE m.median_hotel_price > 0
ORDER BY value_rank;

-- 3)Attraction Market Concentration Analysis (HHI Index Proxy)
create table city_attraction_market_concentration as
WITH CityTotals AS (
    SELECT 
        city_key, 
        SUM(NVL(review_count, 0)) AS total_city_reviews
    FROM ATTRACTIONS
    GROUP BY city_key
),
AttractionShares AS (
    SELECT 
        a.city_key, 
        a.headline, 
        NVL(a.review_count, 0) AS review_count,
        ROUND(
            (NVL(a.review_count, 0) / NULLIF(c.total_city_reviews, 0)) * 100, 
            2
        ) AS market_share_pct
    FROM ATTRACTIONS a
    JOIN CityTotals c 
        ON a.city_key = c.city_key
)
SELECT 
    city_key, 
    ROUND(SUM(market_share_pct * market_share_pct), 2) AS hhi_concentration_index,
    CASE 
        WHEN SUM(market_share_pct * market_share_pct) > 2500 THEN 'Highly Concentrated'
        WHEN SUM(market_share_pct * market_share_pct) BETWEEN 1500 AND 2500 THEN 'Moderately Concentrated'
        ELSE 'Diversified Market'
    END AS market_status
FROM AttractionShares
GROUP BY city_key
ORDER BY hhi_concentration_index DESC;

-- 4)Tourist Trap Detector: Popular but Below-Average Restaurants
create table tourist_trap_restaurants as
WITH CityAverages AS (
    SELECT 
        city_key, 
        ROUND(AVG(rating), 2) AS city_avg_rest_rating
    FROM RESTAURANTS
    WHERE rating IS NOT NULL
    GROUP BY city_key
),
RestaurantRanks AS (
    SELECT 
        r.name,
        r.city_key,
        r.city,
        r.rating,
        r.review_count,
        r.price,
        c.city_avg_rest_rating,
        ROUND(r.rating - c.city_avg_rest_rating, 2) AS rating_deficit
    FROM RESTAURANTS r
    JOIN CityAverages c 
        ON r.city_key = c.city_key
    WHERE r.review_count > 1000
      AND r.price IS NOT NULL
      AND r.rating IS NOT NULL
)
SELECT *
FROM RestaurantRanks
WHERE rating_deficit < -0.5
ORDER BY review_count DESC, rating_deficit ASC;

--5) Hotel Price Outlier Detection Using IQR Method
create table hotel_price_outliers as
WITH Quartiles AS (
    SELECT 
        city_key,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_final) 
            OVER (PARTITION BY city_key) AS Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_final) 
            OVER (PARTITION BY city_key) AS Q3
    FROM HOTELS
    WHERE price_final IS NOT NULL
),
IQR_Bounds AS (
    SELECT DISTINCT 
        city_key, 
        Q1, 
        Q3, 
        (Q3 - Q1) AS IQR,
        Q1 - 1.5 * (Q3 - Q1) AS lower_bound, 
        Q3 + 1.5 * (Q3 - Q1) AS upper_bound
    FROM Quartiles
)
SELECT 
    h.hotel_name, 
    h.city_key, 
    h.price_final, 
    i.lower_bound,
    i.upper_bound,
    CASE 
        WHEN h.price_final > i.upper_bound THEN 'High Outlier'
        WHEN h.price_final < i.lower_bound THEN 'Low Outlier'
    END AS outlier_status
FROM HOTELS h
JOIN IQR_Bounds i 
    ON h.city_key = i.city_key
WHERE h.price_final IS NOT NULL
  AND (h.price_final > i.upper_bound OR h.price_final < i.lower_bound)
ORDER BY h.price_final DESC;

-- 6)Flight Price Volatility and Moving Average Trend Analysis
create table flight_price_volatility_trends as
WITH FlightTrends AS (
    SELECT 
        airline_clean,
        destination_city,
        flight_date,
        price,

        AVG(price) OVER(
            PARTITION BY airline_clean, destination_city
            ORDER BY flight_date
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg,

        STDDEV(price) OVER(
            PARTITION BY airline_clean, destination_city
        ) AS price_volatility

    FROM FLIGHTS
    WHERE price IS NOT NULL
)

SELECT 
    airline_clean,
    destination_city,
    flight_date,
    price,
    ROUND(rolling_7d_avg, 2) AS rolling_avg,
    ROUND(price_volatility, 2) AS std_dev_volatility

FROM FlightTrends

WHERE price > rolling_7d_avg + (1.5 * price_volatility)

ORDER BY price_volatility DESC, flight_date;

-- 7)Airline Dynamic Pricing Window Analysis
create table flight_dynamic_pricing_jumps as
WITH DepartureLags AS (
    SELECT 
        origin_city,
        destination_city,
        flight_date,
        departure_time,
        airline_clean,
        price,
        LAG(price, 1) OVER(
            PARTITION BY origin_city, destination_city, flight_date
            ORDER BY departure_time
        ) AS prev_flight_price
    FROM FLIGHTS
    WHERE price IS NOT NULL
)
SELECT 
    origin_city,
    destination_city,
    flight_date,
    departure_time,
    airline_clean,
    price,
    prev_flight_price,
    ROUND(((price - prev_flight_price) / NULLIF(prev_flight_price, 0)) * 100, 2) AS price_jump_pct
FROM DepartureLags
WHERE prev_flight_price IS NOT NULL
ORDER BY ABS(price - prev_flight_price) DESC;

-- 8)Route Monopoly vs Competitive Market Analysis
create table route_market_competition_summary as
WITH RouteCarriers AS (
    SELECT 
        origin_city,
        destination_city,
        COUNT(DISTINCT airline_clean) AS carrier_count,
        AVG(price) AS avg_route_price
    FROM FLIGHTS
    WHERE price IS NOT NULL
    GROUP BY origin_city, destination_city
)
SELECT 
    CASE 
        WHEN carrier_count = 1 THEN 'Monopoly'
        ELSE 'Competitive'
    END AS market_type,
    COUNT(1) AS total_routes,
    ROUND(AVG(avg_route_price), 2) AS avg_price_across_tier
FROM RouteCarriers
GROUP BY 
    CASE 
        WHEN carrier_count = 1 THEN 'Monopoly'
        ELSE 'Competitive'
    END;
    
-- 9)Most Consistent Airlines Based on Price Stability (Coefficient of Variation)
create table airline_price_stability as
WITH AirlineStats AS (
    SELECT 
        airline_clean,
        ROUND(AVG(price), 2) AS mean_price,
        ROUND(STDDEV(price), 2) AS std_dev_price,
        COUNT(1) AS total_flights
    FROM FLIGHTS
    WHERE price IS NOT NULL
    GROUP BY airline_clean
)

SELECT 
    airline_clean,
    mean_price,
    std_dev_price,
    ROUND((std_dev_price / NULLIF(mean_price, 0)), 4) AS coef_variation
FROM AirlineStats
WHERE total_flights > 100
ORDER BY coef_variation ASC;

-- 10)High-Density Fine Dining Cities Analysis
create table city_fine_dining_density as
WITH PremiumFood AS (
    SELECT 
        city_key,
        SUM(CASE WHEN rating >= 4.8 THEN 1 ELSE 0 END) AS premium_restaurants,
        COUNT(1) AS total_restaurants
    FROM RESTAURANTS
    WHERE rating IS NOT NULL
    GROUP BY city_key
)

SELECT 
    city_key,
    premium_restaurants,
    total_restaurants,
    ROUND((premium_restaurants / NULLIF(total_restaurants,0)) * 100, 2) AS fine_dining_density_pct
FROM PremiumFood
WHERE total_restaurants > 50
ORDER BY fine_dining_density_pct DESC;

-- 11)Holistic Trip Cost Baseline Analysis
create table city_trip_cost_baseline as 
WITH BaseFlt AS (
    SELECT DISTINCT
        destination_city AS city_key,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)
            OVER (PARTITION BY destination_city) AS bottom_25_flight
    FROM FLIGHTS
    WHERE price IS NOT NULL
),
BaseHtl AS (
    SELECT DISTINCT
        city_key,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_final)
            OVER (PARTITION BY city_key) AS median_htl
    FROM HOTELS
    WHERE price_final IS NOT NULL
)
SELECT 
    h.city_key,
    ROUND(f.bottom_25_flight, 2) AS flight_est,
    ROUND(h.median_htl * 3, 2) AS htl_3d_est,
    ROUND(f.bottom_25_flight + (h.median_htl * 3), 2) AS total_baseline
FROM BaseHtl h
JOIN BaseFlt f 
    ON h.city_key = f.city_key
ORDER BY total_baseline ASC;

-- 12)Value vs Premium Segmentation Patterns by City
create table city_value_premium_segments as
WITH CityAvgs AS (
    SELECT 
        h.city_key, 
        AVG(h.price_final) AS avg_hotel, 
        AVG(f.price) AS avg_flight
    FROM HOTELS h
    JOIN FLIGHTS f 
        ON h.city_key = f.city_key
    WHERE h.price_final IS NOT NULL
      AND f.price IS NOT NULL
    GROUP BY h.city_key
),
GlobalStats AS (
    SELECT 
        AVG(avg_hotel) AS gl_hotel, 
        AVG(avg_flight) AS gl_flight 
    FROM CityAvgs
)
SELECT 
    c.city_key,
    ROUND(c.avg_hotel, 2) AS avg_hotel,
    ROUND(c.avg_flight, 2) AS avg_flight,
    CASE 
        WHEN c.avg_hotel > g.gl_hotel * 1.3 
         AND c.avg_flight > g.gl_flight * 1.3 THEN 'Luxury / Premium'
        WHEN c.avg_hotel < g.gl_hotel * 0.8 
         AND c.avg_flight < g.gl_flight * 0.8 THEN 'Budget'
        ELSE 'Mid-Tier'
    END AS segment
FROM CityAvgs c
CROSS JOIN GlobalStats g
ORDER BY segment, c.city_key;

-- 13)Market Opportunity Gap Analysis
create table city_market_opportunity_gap as
WITH FlightDemand AS (
    SELECT 
        city_key, 
        COUNT(1) AS inbound_flights
    FROM FLIGHTS
    GROUP BY city_key
),
HotelInv AS (
    SELECT 
        city_key, 
        COUNT(1) AS hotel_count, 
        SUM(review_count) AS total_hotel_reviews
    FROM HOTELS
    GROUP BY city_key
)
SELECT 
    f.city_key, 
    f.inbound_flights, 
    h.hotel_count,
    h.total_hotel_reviews,
    ROUND(f.inbound_flights / NULLIF(h.hotel_count, 0), 2) AS flight_hotel_ratio
FROM FlightDemand f
JOIN HotelInv h 
    ON f.city_key = h.city_key
ORDER BY flight_hotel_ratio DESC;

--14) Multi-Dimensional City Rating Anomaly Detection
create table city_rating_anomalies as
WITH HotelRates AS (
    SELECT 
        city_key,
        AVG(rating) AS h_rt
    FROM HOTELS
    WHERE rating IS NOT NULL
    GROUP BY city_key
),
RestRates AS (
    SELECT 
        city_key,
        AVG(rating) AS r_rt
    FROM RESTAURANTS
    WHERE rating IS NOT NULL
    GROUP BY city_key
),
AttrRates AS (
    SELECT 
        city_key,
        AVG(review_score) AS a_rt
    FROM ATTRACTIONS
    WHERE review_score IS NOT NULL
    GROUP BY city_key
),
CityRates AS (
    SELECT 
        h.city_key,
        h.h_rt,
        r.r_rt,
        a.a_rt
    FROM HotelRates h
    JOIN RestRates r
        ON h.city_key = r.city_key
    JOIN AttrRates a
        ON h.city_key = a.city_key
),
GlobalVar AS (
    SELECT 
        AVG(h_rt) AS m_h,
        STDDEV(h_rt) AS sd_h,
        AVG(r_rt) AS m_r,
        STDDEV(r_rt) AS sd_r,
        AVG(a_rt) AS m_a,
        STDDEV(a_rt) AS sd_a
    FROM CityRates
)
SELECT 
    c.city_key,
    ROUND((c.h_rt - g.m_h) / NULLIF(g.sd_h, 0), 2) AS z_hotel,
    ROUND((c.r_rt - g.m_r) / NULLIF(g.sd_r, 0), 2) AS z_rest,
    ROUND((c.a_rt - g.m_a) / NULLIF(g.sd_a, 0), 2) AS z_attr
FROM CityRates c
CROSS JOIN GlobalVar g
ORDER BY c.city_key;

--15) Ultimate Analyst Master Summary Dashboard by City
create table city_master_summary_dashboard as
WITH Flt AS (
    SELECT 
        city_key,
        AVG(price) AS avg_flt_cost
    FROM FLIGHTS
    WHERE price IS NOT NULL
    GROUP BY city_key
),
Htl AS (
    SELECT 
        city_key,
        AVG(price_final) AS htl_cost,
        AVG(rating) AS htl_rt
    FROM HOTELS
    WHERE price_final IS NOT NULL
       OR rating IS NOT NULL
    GROUP BY city_key
),
Rst AS (
    SELECT 
        city_key,
        AVG(rating) AS rst_rt,
        COUNT(1) AS rst_cnt
    FROM RESTAURANTS
    WHERE rating IS NOT NULL
    GROUP BY city_key
),
Attr AS (
    SELECT 
        city_key,
        AVG(review_score) AS attr_rt,
        SUM(review_count) AS attr_revs
    FROM ATTRACTIONS
    WHERE review_score IS NOT NULL
       OR review_count IS NOT NULL
    GROUP BY city_key
)
SELECT 
    COALESCE(F.city_key, H.city_key, R.city_key, A.city_key) AS city_key,
    ROUND(F.avg_flt_cost, 2) AS flight_px,
    ROUND(H.htl_cost, 2) AS hotel_px,
    ROUND(H.htl_rt, 2) AS hotel_rt,
    ROUND(R.rst_rt, 2) AS rest_rt,
    ROUND(A.attr_rt, 2) AS attr_rt,
    R.rst_cnt,
    A.attr_revs,
    ROUND(
        (NVL(H.htl_rt, 0) + NVL(R.rst_rt, 0) + NVL(A.attr_rt, 0)) / 3,
        2
    ) AS macro_score,
    RANK() OVER (
        ORDER BY
            (NVL(H.htl_rt, 0) + NVL(R.rst_rt, 0) + NVL(A.attr_rt, 0)) DESC
    ) AS global_rank
FROM Flt F
FULL OUTER JOIN Htl H
    ON F.city_key = H.city_key
FULL OUTER JOIN Rst R
    ON COALESCE(F.city_key, H.city_key) = R.city_key
FULL OUTER JOIN Attr A
    ON COALESCE(F.city_key, H.city_key, R.city_key) = A.city_key
ORDER BY global_rank ASC;

-- 16) Flight Price Seasonality Index and Monthly Peak Detector
create table flight_seasonality_index as
WITH MonthlyAverages AS (
    SELECT 
        EXTRACT(MONTH FROM flight_date) AS flight_month,
        COUNT(1) AS monthly_flight_volume,
        ROUND(AVG(price), 2) AS avg_monthly_price
    FROM FLIGHTS
    WHERE price IS NOT NULL AND flight_date IS NOT NULL
    GROUP BY EXTRACT(MONTH FROM flight_date)
),
YearlyBaseline AS (
    SELECT 
        ROUND(AVG(price), 2) AS yearly_avg_price
    FROM FLIGHTS
    WHERE price IS NOT NULL
)
SELECT 
    m.flight_month,
    m.monthly_flight_volume,
    m.avg_monthly_price,
    y.yearly_avg_price,
    ROUND((m.avg_monthly_price / NULLIF(y.yearly_avg_price, 0)), 2) AS seasonality_index,
    CASE
        WHEN m.avg_monthly_price > y.yearly_avg_price * 1.15 THEN 'Peak Season (High Demand)'
        WHEN m.avg_monthly_price < y.yearly_avg_price * 0.85 THEN 'Off-Peak Season (Low Demand)'
        ELSE 'Shoulder Season (Normal)'
    END AS season_type
FROM MonthlyAverages m
CROSS JOIN YearlyBaseline y
ORDER BY m.flight_month;
