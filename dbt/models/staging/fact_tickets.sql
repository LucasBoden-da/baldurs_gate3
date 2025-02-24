with
    fact_tickets as (
        select
            id as ticket_id,
            DATE(created_at) as created_at,
            DATE(first_response_at) as first_response_date,
            DATE(resolved_at) as solve_date,
            ticket_category_id,
            ticket_status_id,
            user_id,
            TIMESTAMP_DIFF(first_response_at, created_at, SECOND) as time_first_response_seconds,
            TIMESTAMP_DIFF(resolved_at, created_at, SECOND) as time_resolution_seconds,
            TIMESTAMP_DIFF(resolved_at, first_response_at, SECOND) as time_from_first_response_to_resolve_seconds
        from
            {{ source('baldursgate3', 'stg_tickets') }} s
    )
select * from fact_tickets
