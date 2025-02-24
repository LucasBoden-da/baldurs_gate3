with
    dim_tickets_categories as (
        select
            id AS ticket_category_id,
            name as tickets_category
        from
            {{ source('baldursgate3', 'stg_tickets_categories') }}
    )
select * from dim_tickets_categories