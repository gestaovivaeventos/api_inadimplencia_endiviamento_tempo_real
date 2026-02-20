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
               WITH
	-- 1. BASE DE FUNDOS
	cte_fundos AS (
		SELECT
			f.id,
			f.nm_fundo,
			f.curso_id,
			f.unidade_id,
			f.consultorplanejamento_id,
			f.consultoratendimento_id,
			f.consultorproducao_id,
			f.tp_curso,
			f.tp_servico,
			f.situacao,
			f.tipocliente_id,
			f.vl_orcamento_contrato,
			f.maf_replanejado,
			f.dt_contrato,
			f.dt_cadastro,
			f.dt_baile
		FROM
			tb_fundo f
			JOIN tb_unidade u ON u.id = f.unidade_id
		WHERE
			f.fl_ativo IS TRUE
			AND u.categoria = '2'
			AND f.tipocliente_id = '15'
			AND COALESCE(f.is_fundo_teste, FALSE) = FALSE
	),
	-- 2. INTEGRANTES BASE
	cte_integrantes AS (
		SELECT
			i.id AS id_integrante,
			i.fundo_id,
			i.nu_status,
			i.fl_ativo
		FROM
			tb_integrante i
			INNER JOIN cte_fundos cf ON i.fundo_id = cf.id
	),
	-- 3. RESUMO ORDENS 
	cte_ordem AS (
		SELECT
			o.integrante_id,
			SUM(
				CASE
					WHEN o.dt_liquidacao IS NULL
					AND (
						o.vl_pago = 0
						OR o.vl_pago IS NULL
					)
					AND o.dt_vencimento < (CURRENT_DATE - 30)
					AND o.ds_mensagem !~~* '%Especial%'
					AND o.ds_mensagem !~~* '%Convite extra%' THEN COALESCE(o.vl_ordem, 0)
					ELSE 0
				END
			) AS val_inadimplencia,
			MAX(
				CASE
					WHEN o.dt_liquidacao IS NULL
					AND (
						o.vl_pago = 0
						OR o.vl_pago IS NULL
					)
					AND o.dt_vencimento < (CURRENT_DATE - 30)
					AND o.ds_mensagem !~~* '%Especial%'
					AND o.ds_mensagem !~~* '%Convite extra%' THEN 1
					ELSE 0
				END
			) AS flag_inadimplente,
			SUM(
				CASE
					WHEN o.dt_liquidacao IS NOT NULL
					AND (
						o.vl_pago > 0
						OR o.vl_pago IS NOT NULL
					)
					AND o.ds_mensagem !~~* '%Especial%'
					AND o.ds_mensagem !~~* '%Convite extra%' THEN o.vl_ordem
					ELSE 0
				END
			) AS val_pagos,
			SUM(
				CASE
					WHEN o.vl_pago > 0 THEN 1
					ELSE 0
				END
			) AS qtde_pagos_q3,
			MAX(
				CASE
					WHEN o.dt_vencimento < (CURRENT_DATE - 30) THEN 1
					ELSE 0
				END
			) AS flag_vencido_30d_q3,
			SUM(
				CASE
					WHEN o.vl_pago > 0
					AND o.ds_mensagem !~~* '%Especial%'
					AND o.ds_mensagem !~~* '%Convite extra%' THEN 1
					ELSE 0
				END
			) AS qtde_pagos_q7,
			MAX(
				CASE
					WHEN o.dt_vencimento < (CURRENT_DATE - 30)
					AND o.vl_pago IS NULL THEN 1
					ELSE 0
				END
			) AS flag_vencido_30d_q7,
			SUM(COALESCE(o.vl_ordem, 0)) AS val_total_ordens
		FROM
			tb_ordem o
			INNER JOIN cte_integrantes ci ON o.integrante_id = ci.id_integrante
		WHERE
			o.fl_ativo IS TRUE
		GROUP BY
			o.integrante_id
	),
	-- 4. RESUMO FINANCEIRO 
	cte_fin AS (
		SELECT
			fd.integrante_id,
			SUM(fd.vl_integrante_financeiro) AS total_deb_futuros
		FROM
			tb_integrante_financeiro fd
			INNER JOIN cte_integrantes ci ON fd.integrante_id = ci.id_integrante
		WHERE
			fd.fl_ativo IS TRUE
			AND fd.tipocobranca_id = 4
			AND fd.dt_vencimento > NOW()
		GROUP BY
			fd.integrante_id
	),
	-- 5. MATEMÁTICA AGREGADA
	cte_metricas AS (
		SELECT
			ci.fundo_id,
			COUNT(ci.id_integrante) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (11, 9, 8, 13)
			) AS integrantes_ativos,
			SUM(ord.val_inadimplencia) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (11, 9, 8, 13, 15)
			) AS total_inadimplencia,
			COUNT(ci.id_integrante) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (11, 9, 8, 13, 15)
					AND ord.flag_inadimplente = 1
			) AS total_inadimplentes,
			SUM(ord.val_pagos) FILTER (
				WHERE
					ci.nu_status NOT IN (11, 9, 8, 13)
			) AS tt_pagos,
			COUNT(ci.id_integrante) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (10, 11, 9, 8, 13, 14)
					AND ord.qtde_pagos_q3 = 0
					AND ord.flag_vencido_30d_q3 = 1
			) AS nunca_pagaram,
			SUM(ord.val_total_ordens) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (10, 11, 9, 8, 13, 14)
					AND ord.qtde_pagos_q3 = 0
					AND ord.flag_vencido_30d_q3 = 1
			) AS vl_nunca_pagaram,
			SUM(fin.total_deb_futuros) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (11, 9, 8, 13)
			) AS parc_deb_futuros,
			COUNT(ci.id_integrante) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (11, 9, 8, 13)
					AND fin.total_deb_futuros > 0
			) AS integrantes_endividados,
			SUM(fin.total_deb_futuros) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (11, 9, 8, 13, 15)
					AND ord.qtde_pagos_q7 = 0
					AND ord.flag_vencido_30d_q7 = 1
			) AS end_nuca_pagaram,
			COUNT(ci.id_integrante) FILTER (
				WHERE
					ci.fl_ativo IS TRUE
					AND ci.nu_status NOT IN (11, 9, 8, 13, 15)
					AND ord.qtde_pagos_q7 = 0
					AND ord.flag_vencido_30d_q7 = 1
					AND fin.total_deb_futuros > 0
			) AS deb_nunca_pagaram
		FROM
			cte_integrantes ci
			LEFT JOIN cte_ordem ord ON ci.id_integrante = ord.integrante_id
			LEFT JOIN cte_fin fin ON ci.id_integrante = fin.integrante_id
		GROUP BY
			ci.fundo_id
	)
	-- 6. RELATÓRIO FINAL ORGANIZADO
SELECT
	CASE
		WHEN u.nm_unidade = 'Campos' THEN 'Itaperuna Muriae'
		ELSE u.nm_unidade
	END AS nm_unidade,
	f.id AS id_fundo,
	f.nm_fundo,
	c.nm_curso AS curso_fundo,
	CASE f.tp_servico
		WHEN '1' THEN 'Pacote'
		WHEN '2' THEN 'Assessoria'
		WHEN '3' THEN 'Super Integrada'
	END AS tp_servico,
	CASE f.situacao
		WHEN 1 THEN 'Não mapeado'
		WHEN 2 THEN 'Mapeado'
		WHEN 3 THEN 'Em negociação'
		WHEN 4 THEN 'Concorrente'
		WHEN 5 THEN 'Comum'
		WHEN 6 THEN 'Juntando'
		WHEN 7 THEN 'Junção'
		WHEN 8 THEN 'Unificando'
		WHEN 9 THEN 'Unificado'
		WHEN 10 THEN 'Rescindindo'
		WHEN 11 THEN 'Rescindido'
		WHEN 12 THEN 'Realizado'
		WHEN 13 THEN 'Desistente'
		WHEN 14 THEN 'Pendente'
	END AS situacao_fundo,
	CASE f.tipocliente_id
		WHEN '7' THEN 'EMPRESARIAL'
		WHEN '14' THEN 'FRANQUIAS'
		WHEN '15' THEN 'FUNDO DE FORMATURA'
		WHEN '16' THEN 'OUTROS'
	END AS tipo_cliente_fundo,
	u_relac.nome AS consultor_relacionamento,
	CASE
		WHEN f.dt_contrato IS NULL
		OR f.dt_contrato > f.dt_cadastro THEN f.dt_cadastro
		ELSE f.dt_contrato
	END AS dt_contrato_fundo,
	f.dt_cadastro,
	f.dt_baile,
	COALESCE(m.total_inadimplencia, 0) AS total_inadimplencia,
	COALESCE(m.total_inadimplentes, 0) AS total_inadimplentes,
	COALESCE(m.integrantes_ativos, 0) AS integrantes_ativos,
	COALESCE(m.nunca_pagaram, 0) AS nunca_pagaram,
	-- Regra condicional solicitada: se endividados = 0, traz inadimplentes
	CASE
		WHEN COALESCE(m.integrantes_endividados, 0) = 0 THEN COALESCE(m.total_inadimplentes, 0)
		ELSE m.integrantes_endividados
	END AS integrantes_endividados,
	COALESCE(m.parc_deb_futuros, 0) AS parc_deb_futuross,
	COALESCE(m.tt_pagos, 0) AS tt_pagos,
	NULL AS fundo_venda_pos, -- Coluna em branco (não mapeada nas consultas originais)
	f.vl_orcamento_contrato AS maf_atual,
	CASE f.tp_curso
		WHEN 1 THEN 'Ens Médio'
		WHEN 2 THEN 'Segundo grau'
		WHEN 3 THEN 'Técnico'
		WHEN 4 THEN 'Graduação'
		WHEN 5 THEN 'Outros'
		WHEN 6 THEN 'Tecnólogo'
		WHEN 7 THEN 'Militar'
		WHEN 8 THEN 'Colação'
	END AS tp_curso,
	NULL AS cluster_raiox, -- Coluna em branco
	COALESCE(m.vl_nunca_pagaram, 0) AS vl_nunca_pagaram,
	COALESCE(m.end_nuca_pagaram, 0) AS end_nuca_pagaram,
	COALESCE(m.deb_nunca_pagaram, 0) AS deb_nunca_pagaram,
	u_atend.nome AS atendimento,
	u_prod.nome AS producao,
	u_plan.nome AS planejamento
FROM
	cte_fundos f
	JOIN tb_unidade u ON u.id = f.unidade_id
	JOIN tb_curso c ON c.id = f.curso_id
	LEFT JOIN tb_usuario u_relac ON u_relac.id = f.consultorplanejamento_id
	AND u_relac.enabled IS TRUE
	LEFT JOIN tb_usuario u_atend ON u_atend.id = f.consultoratendimento_id
	LEFT JOIN tb_usuario u_prod ON u_prod.id = f.consultorproducao_id
	LEFT JOIN tb_usuario u_plan ON u_plan.id = f.consultorplanejamento_id
	LEFT JOIN cte_metricas m ON m.fundo_id = f.id
ORDER BY
	nm_unidade,
	f.nm_fundo
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
