with tickets_per_status as (
    select 
        count(*) as created,
        countif(first_response_at is not null) as responded, 
        countif(resolved_at is not null) as solved
    from
        `baldursgate3`.`baldursgate3`.`stg_tickets` s
)
select 'created' as status, created as count from tickets_per_status
union all
select 'responded' as status, responded as count from tickets_per_status
union all
select 'solved' as status, solved as count from tickets_per_status