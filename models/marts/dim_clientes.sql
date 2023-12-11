with 
    stg_clientes as (
        select 
            id_cliente
            , cliente_nome_empresa
            , cliente_nome
            , titulo_cliente
            , endereco_cliente
            , cidade_cliente
            , regiao_cliente
            , cep_cliente
            , pais_cliente
        from {{ ref('stg_erp__clientes') }}
    )

select * 
from stg_clientes