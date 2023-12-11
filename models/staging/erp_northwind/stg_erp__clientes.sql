with
    fonte_clientes as(
        select 
            cast(customer_id as string) as id_cliente
            , cast(company_name as string) as cliente_nome_empresa
            , cast(contact_name as string) as cliente_nome
            , cast(contact_title as string) as titulo_cliente
            , cast(address as string) as endereco_cliente
            , cast(city as string) as cidade_cliente
            , cast(region as string) as regiao_cliente
            , postal_code as cep_cliente
            , cast(country as string) as pais_cliente
           -- , cast(phone as string) as numero_cliente
           -- , cast(fax as string) as 
        
        from {{ source('erp', 'customers') }}
    )

select *
from fonte_clientes