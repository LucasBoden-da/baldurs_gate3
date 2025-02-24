with
    dim_countries as (
        select
            id AS id_countries,
            name as country,
            continent
        from
            `baldursgate3`.`baldursgate3`.`stg_countries`
    )
select * from dim_countries