with
    dim_users as (
        select
            id AS user_id,
            referral_id,
            country_id
        from
            {{ source('baldursgate3', 'stg_users') }}
    )
select * from dim_users