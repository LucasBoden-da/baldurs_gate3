with
    dim_referrals as (
        select
            id AS referral_id,
            name as referral
        from
            `baldursgate3`.`baldursgate3`.`stg_referrals`
    )
select * from dim_referrals