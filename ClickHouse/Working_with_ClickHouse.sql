CREATE TABLE user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

CREATE TABLE user_events_agg(
	event_date DateTime,
	event_type String,
	uniq_users_state AggregateFunction(uniq, UInt32),
	points_spent_state AggregateFunction(sum, UInt32),
	action_count_state AggregateFunction(count, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY(event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW user_events_agg_mv
TO user_events_agg AS 
SELECT toDate(event_time) AS event_date,
	   event_type,
	   uniqState(user_id) AS uniq_users_state,
	   sumState(points_spent) AS points_spent_state,
	   countState() AS action_count_state
FROM user_events
GROUP BY event_date, event_type;

INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

--Запрос вывода
SELECT event_date, event_type,
uniqMerge(uniq_users_state),
sumMerge(points_spent_state),
countMerge(action_count_state)
FROM user_events_agg
GROUP BY event_date, event_type


WITH
first_events AS (
    SELECT
        user_id,
        toDate(MIN(event_time)) AS first_date
    FROM user_events
    GROUP BY user_id
),
returned_users AS (
    SELECT
        fe.user_id,
        fe.first_date
    FROM first_events fe
    INNER JOIN user_events ue
        ON fe.user_id = ue.user_id
        AND toDate(ue.event_time) > fe.first_date
        AND toDate(ue.event_time) <= fe.first_date + 7
),
new_users_per_day AS (
    SELECT
        first_date AS day_0,
        COUNT(*) AS total_users_day_0
    FROM first_events
    GROUP BY first_date
),
returned_per_day AS (
    SELECT
        first_date AS day_0,
        COUNT(DISTINCT user_id) AS returned_in_7_days
    FROM returned_users
    GROUP BY first_date
)
SELECT
    d0.day_0,
    d0.total_users_day_0,
    COALESCE(r7.returned_in_7_days, 0) AS returned_in_7_days,
    round(COALESCE(r7.returned_in_7_days, 0) / d0.total_users_day_0 * 100, 2) AS retention_7d_percent
FROM new_users_per_day d0
LEFT JOIN returned_per_day r7 ON d0.day_0 = r7.day_0
ORDER BY day_0;