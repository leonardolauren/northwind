with
    fonte_categorias as(
        select 
            cast(CustomerID as int) as id_cliente
        from {{ source('boaboa', 'Customer') }}
    )

select *
from fonte_categorias