import os
import warnings
import pandas as pd
from utils import filtrar_segmento

warnings.filterwarnings('ignore')


class AnaliseEstabelecimentos:
    """
    Classe para análise de estabelecimentos baseada em CNAE e situação cadastral.
    Permite carregar, pré-processar e gerar relatórios em CSV.
    """

    def __init__(self, alvos: list, output_dir: str = 'output'):
        """
        Inicializa a análise com os CNAEs de interesse.

        Args:
            alvos (list): Lista de CNAEs alvo.
            output_dir (str): Diretório para salvar os relatórios.
        """
        self.alvos = alvos
        self.est_df: pd.DataFrame | None = None
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def carregar_preprocessar(self, colunas_estabelecimento: list) -> None:
        """
        Carrega e pré-processa os dados de estabelecimentos,
        realizando junções e ajustes de tipos.
        """
        est_df = pd.read_parquet("./data/ESTABELECIMENTO.parquet")
        cnae_df = pd.read_csv(
            "./data/CNAE.csv",
            encoding="latin1",
            sep=";",
            names=['cnae_fiscal_principal', 'atividade_economica']
        )
        municipios_df = pd.read_csv(
            "./data/MUNICIPIOS.csv",
            sep=";",
            names=['codigo_municipio', 'cidade']
        )
        motivos_df = pd.read_csv(
            './data/MOTIVOS.csv',
            sep=';',
            names=['codigo_motivo_situacao_cadastral', 'motivo_situacao_cadastral']
        )

        # Renomear colunas
        est_df.columns = colunas_estabelecimento

        # Ajuste de valores ausentes e tipos
        est_df['codigo_municipio'] = est_df['codigo_municipio'].fillna(0)
        cols_int = ['cnae_fiscal_principal', 'situacao_cadastral', 'codigo_municipio']
        est_df[cols_int] = est_df[cols_int].astype(int)

        est_df = est_df.drop_duplicates()

        # Junções
        est_df = est_df.merge(municipios_df, on='codigo_municipio', how='left')
        est_df = est_df.merge(cnae_df, on='cnae_fiscal_principal', how='left')
        est_df = est_df.merge(motivos_df, on='codigo_motivo_situacao_cadastral', how='left')

        # Conversão de datas
        est_df['data_inicio_atividade'] = pd.to_datetime(
            est_df['data_inicio_atividade'].astype(str),
            format='%Y%m%d',
            errors='coerce'
        )
        est_df['data_situacao_cadastral'] = pd.to_datetime(
            est_df['data_situacao_cadastral'].astype(str),
            format='%Y%m%d',
            errors='coerce'
        )

        # Filtrar segmento alvo ativo
        self.est_df = est_df
        
    def contar_empresas_ativas_segmento(self) -> None:
        """
        Conta o número total de empresas ativas do segmento alvo.
        """

        df = filtrar_segmento(df=self.est_df, alvos=self.alvos, situacao=2)

        total_geral = df.shape[0]
        pd.DataFrame({'total_geral': [total_geral]}).to_csv(
            os.path.join(self.output_dir, "empresas_ativas.csv"),
            index=False
        )

    def distribuicao_geografica_empresas(self) -> None:
        """
        Gera a distribuição geográfica de empresas ativas por cidade e estado,
        considerando a data de início da atividade.
        """
        df = filtrar_segmento(df=self.est_df, alvos=self.alvos, situacao=2)

        cidades_estados = (
            df.groupby([df['data_inicio_atividade'].dt.to_period('M'), 'cidade', 'uf'])
              .size()
              .sort_values(ascending=False)
              .reset_index(name='quantidade')
        )

        cidades_estados.to_csv(
            os.path.join(self.output_dir, "estados_cidades_mais_concentrados.csv"),
            index=False
        )

    def cnaes_mais_comuns(self) -> None:
        """
        Lista os CNAEs mais comuns entre as empresas ativas.
        """
        df = (
            filtrar_segmento(df=self.est_df, alvos=self.alvos, situacao=2)
            .groupby(['uf', 'cnae_fiscal_principal', 'atividade_economica'])
            .size()
            .reset_index(name='quantidade')
        )

        df.to_csv(
            os.path.join(self.output_dir, "cnaes_mais_comuns_ativos.csv"),
            index=False
        )

    def tendencia_abertura_empresas(self) -> None:
        """
        Analisa a tendência de abertura de empresas nos últimos 10 anos.
        """
        setor_df = self.est_df[self.est_df['cnae_fiscal_principal'].isin(self.alvos)]
        data_max = setor_df['data_inicio_atividade'].max()
        data_limite = data_max - pd.DateOffset(years=10)

        df = setor_df[setor_df['data_inicio_atividade'] >= data_limite]
        df_counts = (
            df.groupby([df['data_inicio_atividade'].dt.to_period('M'), 'uf'])
              .size()
              .reset_index(name='quantidade')
        )
        df_counts['data_inicio_atividade'] = df_counts['data_inicio_atividade'].dt.to_timestamp()

        df_counts.to_csv(
            os.path.join(self.output_dir, "tendencia_abertura.csv"),
            index=False
        )

    def motivos_inatividade_empresas(self) -> None:
        """
        Lista os principais motivos de inatividade de empresas do segmento alvo.
        """
        df = filtrar_segmento(df=self.est_df, alvos=self.alvos, situacao=[3, 4, 8])

        df_counts = (
            df.groupby([df['data_situacao_cadastral'].dt.to_period('M'),
                        'motivo_situacao_cadastral', 'uf'])
              .size()
              .reset_index(name='quantidade')
        )
        df_counts.columns = ['data_situacao_cadastral', 'motivo', 'uf', 'quantidade']

        df_counts.to_csv(
            os.path.join(self.output_dir, "motivos_inatividade.csv"),
            index=False
        )