with 
    fonte_ordens as (
        select  

            cast(order_id as int) as id_pedido
            , cast(customer_id as string) as id_cliente
            , cast(employee_id as int) as id_funcionario
            , cast(order_date as string) as data_do_pedido
            , cast(required_date as date) as dt_requerida_entrega
            , cast(shipped_date as date) as data_do_envio
            , cast(ship_via as int) as id_transportadora
            , cast(freight as numeric) as frete
            , cast(ship_name as string) as destinatario
            , cast(ship_address as string) as endereco_destinatario
            , cast(ship_city as string) as cidade_destinatario
            , cast(ship_region as string) as regiao_destinatario
            , cast(ship_postal_code as string) as dep_destinatario
            , cast(ship_country as string) as pais_destinatario



        from {{ source('erp', 'orders') }}
    )

select * from fonte_ordens
