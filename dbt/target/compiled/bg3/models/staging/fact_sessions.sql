with
    fact_sessions as (
        select
            id as session_id,
            device_id,
            country_access_id,
            user_id,
            DATE(start_date) as start_date,
            DATE(date_trunc(start_date,week)) as week,
            TIMESTAMP_DIFF(s.end_date, s.start_date, SECOND) as time_spent_seconds
        from
            `baldursgate3`.`baldursgate3`.`stg_sessions` s
    )
select * from fact_sessions