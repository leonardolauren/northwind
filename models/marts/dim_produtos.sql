

with 
    stg_categorias as (
        select *
        from {{ ref('stg_erp__categorias') }}
    )

    , stg_fornecedores as (
        select *
        from {{ ref('stg_erp__fornecedores') }}
    )

    , stg_produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )

    , joined_tabelas as (
        select
        stg_produtos.id_produto
        , stg_produtos.id_fornecedor
        , stg_produtos.id_categoria
        , stg_produtos.nome_produto
        , stg_produtos.quantidade_por_unidade
        , stg_produtos.preco_por_unidade
        , stg_produtos.unidades_em_estoque
        , stg_produtos.unidades_por_ordem
        , stg_produtos.nivel_reabastecimento
        , stg_produtos.eh_descontinuado
        , stg_categorias.nome_categoria
        , stg_fornecedores.nome_fornecedor
        , stg_fornecedores.pais_fornecedor
        
        from stg_produtos
        left join stg_categorias on
            stg_produtos.id_categoria = stg_categorias.id_categoria
        left join stg_fornecedores on
            stg_produtos.id_fornecedor = stg_fornecedores.id_fornecedor
    )

select *
from joined_tabelas