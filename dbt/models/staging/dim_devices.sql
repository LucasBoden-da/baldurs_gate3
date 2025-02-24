with
    dim_devices as (
        select
            id AS id_devices,
            name AS device_name
        from
            {{ source('baldursgate3', 'stg_devices') }}

    )
select * from dim_devices