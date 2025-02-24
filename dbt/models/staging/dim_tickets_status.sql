with
    dim_tickets_status as (
        select
            id AS ticket_status_id,
            description as tickets_status
        from
            {{ source('baldursgate3', 'stg_tickets_status') }}
    )
select * from dim_tickets_status