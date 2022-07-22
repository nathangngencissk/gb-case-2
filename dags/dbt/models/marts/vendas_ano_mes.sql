-- vendas_ano_mes.sql

with 

vendas as (

   select * from {{ ref('stg_base_vendas__vendas') }}

),
 
aggregate_vendas_per_ano_mes as (
   
   select       sum(qtd_venda) as qtd_total,
                cast(extract(year from data_venda) as string) as ano,
                lpad(cast(extract(month from data_venda) as string),2,'0') as mes
    from        vendas
    group by    2,3
    order by    ano desc, mes desc

)
 
select * from aggregate_vendas_per_ano_mes