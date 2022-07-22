-- vendas_marca_linha.sql

with 

vendas as (

   select * from {{ ref('stg_base_vendas__vendas') }}

),
 
aggregate_vendas_per_marca_linha as (
   
   select       sum(qtd_venda) as qtd_total,
                id_marca,
                nome_marca,
                id_linha,
                nome_linha
    from        vendas
    group by    2,3,4,5
    order by    qtd_total desc

)
 
select * from aggregate_vendas_per_marca_linha