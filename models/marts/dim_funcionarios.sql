with 
    stg_funcionarios as (
        select 
            id_funcionario
            , id_gerente
            , nome_funcionario
            , cargo_funcionario
            , dt_nascimento_func
            , dt_contratacao
            , cidade_func
            , regiao_func
            , pais_func
            , informacoes_func
        from {{ ref('stg_erp__funcionarios') }}
    )
    
    , self_join_func as (
        select
        funcionario.id_funcionario
            , funcionario.id_gerente
            , funcionario.nome_funcionario
            , funcionario.cargo_funcionario
            , funcionario.dt_nascimento_func
            , funcionario.dt_contratacao
            , funcionario.cidade_func
            , funcionario.regiao_func
            , funcionario.pais_func
            , funcionario.informacoes_func
            , gerente.nome_funcionario as nome_gerente
        from stg_funcionarios as funcionario
        left join stg_funcionarios as gerente on
        funcionario.id_gerente = gerente.id_funcionario  
    )

select * 
from self_join_func