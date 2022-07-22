-- vendas_linha_ano_mes.sql

with 

vendas as (

   select * from {{ ref('stg_base_vendas__vendas') }}

),
 
aggregate_vendas_per_ano_mes_linha as (
   
   select       sum(qtd_venda) as qtd_total,
                id_linha,
                nome_linha,
                cast(extract(year from data_venda) as string) as ano,
                lpad(cast(extract(month from data_venda) as string),2,'0') as mes
    from        vendas
    group by    2,3,4,5
    order by    ano desc, mes desc, nome_linha asc

)
 
select * from aggregate_vendas_per_ano_mes_linha