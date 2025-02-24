with
    dim_devices as (
        select
            id AS id_devices,
            name AS device_name
        from
            `baldursgate3`.`baldursgate3`.`stg_devices`

    )
select * from dim_devices