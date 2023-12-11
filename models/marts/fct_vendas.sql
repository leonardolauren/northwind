with
    funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , int_vendas as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )

    , clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , joined_tabelas as (
        select
            int_vendas.sk_pedido_item
            , int_vendas.id_pedido
            , int_vendas.id_cliente
            , int_vendas.id_funcionario
            , int_vendas.id_produto
            , int_vendas.id_transportadora
            , int_vendas.data_do_pedido
            , int_vendas.dt_requerida_entrega
            , int_vendas.data_do_envio
            , int_vendas.frete
            , int_vendas.destinatario
            , int_vendas.endereco_destinatario
            , int_vendas.cidade_destinatario
            , int_vendas.regiao_destinatario
            , int_vendas.dep_destinatario
            , int_vendas.pais_destinatario
            , int_vendas.preco_da_unidade
            , int_vendas.quantidade
            , int_vendas.desconto_perc
            , produtos.nome_produto
            , produtos.eh_descontinuado
            , produtos.nome_categoria
            , produtos.nome_fornecedor
            , produtos.pais_fornecedor
            , funcionarios.nome_gerente
            , funcionarios.nome_funcionario
            , funcionarios.cargo_funcionario
            , funcionarios.dt_nascimento_func
            , funcionarios.dt_contratacao
            , clientes.cliente_nome
        from int_vendas
        left join produtos on
            int_vendas.id_produto = produtos.id_produto
        left join funcionarios on
            int_vendas.id_funcionario = funcionarios.id_funcionario
        left join clientes on
            int_vendas.id_cliente = clientes.id_cliente
    )

    , tranformacoes as (
        select *
        , quantidade * preco_da_unidade as total_bruto
        , quantidade * preco_da_unidade * (1 - desconto_perc) as total_liquido
        , case 
            when desconto_perc > 0 then 'sim'
            else 'nao'
            end as teve_desconto
            , frete / count(id_pedido) over (partition by id_pedido) as frete_ponderado
        from joined_tabelas
    )
    , select_final as (
        select 
            sk_pedido_item
            , id_pedido
            , id_cliente
            , id_funcionario
            , id_produto
            , id_transportadora
            , data_do_pedido
            , dt_requerida_entrega
            , data_do_envio
            , destinatario
            , endereco_destinatario
            , cidade_destinatario
            , regiao_destinatario
            , dep_destinatario
            , pais_destinatario
            , preco_da_unidade
            , quantidade
            , desconto_perc
            , total_bruto
            , total_liquido
            , teve_desconto
            , frete_ponderado
            , nome_produto
            , eh_descontinuado
            , nome_categoria
            , nome_fornecedor
            , pais_fornecedor
            , nome_gerente
            , nome_funcionario
            , cargo_funcionario
            , dt_nascimento_func
            , dt_contratacao
            , cliente_nome
      from tranformacoes
    )

select *
from select_final