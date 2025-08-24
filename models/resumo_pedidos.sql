with
    itens_vendidos as (
        select pedido_id, sum(quantidade) as itens_vendidos
        from {{ source("dbt", "itens_pedidos") }}
        group by pedido_id

    )
select
    p.pedido_id,
    c.cliente_id,
    c.grupo,
    c.estado,
    format_date('%Y-%m-%d', date(p.data_pedido)) as data_pedido,
    extract(year from date(p.data_pedido)) as ano_pedido,
    extract(month from date(p.data_pedido)) as mes_pedido,
    case
        when extract(month from date(p.data_pedido)) between 1 and 3
        then 'T1'
        when extract(month from date(p.data_pedido)) between 4 and 6
        then 'T2'
        when extract(month from date(p.data_pedido)) between 7 and 9
        then 'T3'
        when extract(month from date(p.data_pedido)) between 10 and 12
        then 'T4'
    end as trimestre,
    pr.itens_vendidos,
    p.status,
    p.valor_total
from {{ source("dbt", "pedidos") }} as p
left join {{ source("dbt", "clientes") }} as c on p.cliente_id = c.cliente_id
left join itens_vendidos as pr on p.pedido_id = pr.pedido_id
