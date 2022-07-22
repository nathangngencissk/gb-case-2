-- stg_base_vendas__vendas.sql

with

source as (

    select * from {{ source('base_vendas','base_vendas') }}

),

renamed as (

    select
        -- ids
        ID_MARCA as id_marca,
        ID_LINHA as id_linha,

        -- strings
        MARCA as nome_marca,
        LINHA as nome_linha,

        -- numerics
        QTD_VENDA as qtd_venda,

        -- timestamps
        DATA_VENDA as data_venda,
        cast(DATA_VENDA as timestamp) as created_at

    from source

)

select * from renamed