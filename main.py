from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool = None
try:
    pool = SimpleConnectionPool(
        minconn=1, maxconn=10,
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DB"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), cursor_factory=RealDictCursor
    )
except psycopg2.OperationalError as e:
    print(f"ERRO CRÍTICO: Falha ao inicializar o pool de conexões. {e}")

@app.get("/")
def health_check():
    return {"status": "ok"}

@app.get("/dados")
def obter_dados(limit: int = 5000, offset: int = 0):
    if not pool:
        raise HTTPException(status_code=503, detail="Serviço indisponível: pool de conexões falhou.")

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            query = """
                SELECT
    CASE 
        WHEN u.nm_unidade = 'Campos' THEN 'Itaperuna Muriae'
        ELSE u.nm_unidade 
    END AS nm_unidade,
    f.id AS id_fundo,
    f.nm_fundo,
    f.dt_baile,
    
    CASE f.tp_servico
        WHEN '1' THEN 'Pacote'
        WHEN '2' THEN 'Assessoria'
        WHEN '3' THEN 'Super Integrada'
    END AS tp_servico,
    
    CASE f.tp_curso
        WHEN 1 THEN 'Ens Médio' WHEN 2 THEN 'Segundo grau' WHEN 3 THEN 'Técnico'
        WHEN 4 THEN 'Graduação' WHEN 5 THEN 'Outros' WHEN 6 THEN 'Tecnólogo'
        WHEN 7 THEN 'Militar' WHEN 8 THEN 'Colação'
    END AS tipo_curso,
    
    c.nm_curso AS curso_fundo,
    
    CASE f.situacao
        WHEN 1 THEN 'Não mapeado' WHEN 2 THEN 'Mapeado' WHEN 3 THEN 'Em negociação'
        WHEN 4 THEN 'Concorrente' WHEN 5 THEN 'Comum' WHEN 6 THEN 'Juntando'
        WHEN 7 THEN 'Junção' WHEN 8 THEN 'Unificando' WHEN 9 THEN 'Unificado'
        WHEN 10 THEN 'Rescindindo' WHEN 11 THEN 'Rescindido' WHEN 12 THEN 'Realizado'
        WHEN 13 THEN 'Desistente' WHEN 14 THEN 'Pendente'
    END AS situacao_fundo,

    CASE f.tipocliente_id
        WHEN '7' THEN 'EMPRESARIAL' WHEN '14' THEN 'FRANQUIAS'
        WHEN '15' THEN 'FUNDO DE FORMATURA' WHEN '16' THEN 'OUTROS' WHEN '17' THEN 'PRE EVENTO'
    END AS tipo_cliente_fundo,

    COALESCE(LEAST(f.dt_contrato, f.dt_cadastro), f.dt_cadastro) AS dt_contrato_fundo,
    f.dt_cadastro,
    
    u_atend.nome AS consultor_atendimento,
    u_prod.nome AS consultor_producao,
    u_plan.nome AS consultor_planejamento,

    f.num_alunos_turma AS tat_inicial,
    f.integrantes_previstos_contrato AS mac_inicial,
    f.vl_orcamento_contrato AS maf_inicial,
    f.tat_replanejado,
    f.mac_replanejado,
    f.maf_replanejado,
    fc_grup.id AS id_juncao,

    COALESCE(stats_geral.integrantes_ativos, 0) AS integrantes_ativos,
    COALESCE(stats_geral.vvr_ativos, 0) AS vvr_ativos,
    COALESCE(stats_geral.total_desligamentos, 0) AS total_desligamentos,
    
    COALESCE(stats_fin.total_inadimplentes, 0) AS total_inadimplentes,
    COALESCE(stats_fin.int_nunca_pagaram, 0) AS int_nunca_pagaram,
    
    COALESCE(stats_evt.aderidos_principal, 0) AS aderidos_principal

FROM tb_fundo f
    JOIN tb_unidade u ON u.id = f.unidade_id
    JOIN tb_curso c ON c.id = f.curso_id
    LEFT JOIN tb_grupo_fundos_correlatos fc_grup ON f.id_grupo_fundos_correlatos = fc_grup.id
    LEFT JOIN tb_usuario u_atend ON u_atend.id = f.consultoratendimento_id
    LEFT JOIN tb_usuario u_prod ON u_prod.id = f.consultorproducao_id
    LEFT JOIN tb_usuario u_plan ON u_plan.id = f.consultorplanejamento_id

    LEFT JOIN LATERAL (
        SELECT 
            COUNT(*) FILTER (
                WHERE i.fl_ativo IS TRUE 
                AND (i.nu_status NOT IN (11, 9, 8, 13) OR i.nu_status IS NULL)
            ) AS integrantes_ativos,
            
            SUM(fcota.vl_plano) FILTER (
                WHERE i.fl_ativo IS TRUE 
                AND (i.nu_status NOT IN (11, 9, 8, 13) OR i.nu_status IS NULL)
            ) AS vvr_ativos,
            
            COUNT(*) FILTER (
                WHERE i.fl_ativo = FALSE
                AND i.dt_desligamento IS NOT NULL
                AND i.dt_desligamento > '2010-12-31'
                AND i.nu_status NOT IN (5,7,8,9,12,13,14)
            ) AS total_desligamentos

        FROM tb_integrante i
        LEFT JOIN tb_fundo_cota fcota ON fcota.cota_id = i.cota_id AND fcota.fundo_id = i.fundo_id
        WHERE i.fundo_id = f.id
    ) stats_geral ON TRUE

    LEFT JOIN LATERAL (
        SELECT 
            COUNT(*) FILTER (WHERE resumo_int.is_inadimplente = 1) AS total_inadimplentes,
            
            COUNT(*) FILTER (
                WHERE resumo_int.is_inadimplente = 1    
                  AND resumo_int.total_pago = 0         
                  AND resumo_int.status_valido_nunca_pagou = 1 
            ) AS int_nunca_pagaram
        FROM (
            SELECT 
                i.id,
                CASE WHEN i.nu_status NOT IN (10, 11, 9, 8, 13, 14) THEN 1 ELSE 0 END as status_valido_nunca_pagou,
                
                SUM(CASE 
                    WHEN o.vl_pago > 0 
                         AND o.ds_mensagem NOT ILIKE '%%Especial%%' 
                         AND o.ds_mensagem NOT ILIKE '%%Convite extra%%'
                    THEN 1 ELSE 0 END) AS total_pago,
                
                MAX(CASE 
                    WHEN o.dt_vencimento < (CURRENT_DATE - 30)
                         AND o.dt_liquidacao IS NULL 
                         AND (o.vl_pago IS NULL OR o.vl_pago = 0)
                         AND o.ds_mensagem NOT ILIKE '%%Especial%%' 
                         AND o.ds_mensagem NOT ILIKE '%%Convite extra%%'
                         AND o.fl_ativo IS TRUE
                    THEN 1 ELSE 0 END) AS is_inadimplente
            FROM tb_integrante i
            JOIN tb_ordem o ON o.integrante_id = i.id
            WHERE i.fundo_id = f.id
              AND i.fl_ativo IS TRUE
              AND i.nu_status NOT IN (11, 9, 8, 13)
            GROUP BY i.id
        ) resumo_int
    ) stats_fin ON TRUE

    LEFT JOIN LATERAL (
        SELECT COUNT(DISTINCT ei.integrante_id) AS aderidos_principal
        FROM tb_evento_contratado ec
        JOIN tb_evento_integrante ei ON ei.eventocontratado_id = ec.id
        JOIN tb_integrante i_evt ON i_evt.id = ei.integrante_id
        WHERE ec.fundo_id = f.id
          AND ec.has_evento_principal IS TRUE
          AND i_evt.fl_ativo IS TRUE
          AND i_evt.nu_status NOT IN (11, 9, 8, 13)
    ) stats_evt ON TRUE

WHERE
    f.fl_ativo IS TRUE
    AND u.categoria = '2'
    AND f.tipocliente_id IN(15, 17)
    AND COALESCE(f.is_fundo_teste, 'False') = 'False'

ORDER BY 
    u.nm_unidade, f.nm_fundo 
LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset))
            dados = cursor.fetchall()
        
        return {"dados": dados}
    except Exception as e:
        # Isso vai te ajudar a ver o erro real no log do Vercel se acontecer de novo
        print(f"Erro na query: {e}") 
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
    finally:
        if conn:
            pool.putconn(conn)
