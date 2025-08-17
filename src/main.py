from analise_estabelecimentos import AnaliseEstabelecimentos

alvos = [4721102, 5611201, 5611203, 5611204, 5611205, 5620104]
colunas_estabelecimento = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "codigo_motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "codigo_municipio",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial"
]

analise = AnaliseEstabelecimentos(alvos)

analise.carregar_preprocessar(colunas_estabelecimento=colunas_estabelecimento)
analise.distribuicao_geografica_empresas()
analise.cnaes_mais_comuns()
analise.tendencia_abertura_empresas()
analise.motivos_inatividade_empresas()