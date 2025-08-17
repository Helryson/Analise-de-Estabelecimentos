import pandas as pd

def filtrar_segmento(df, alvos, situacao=None):
    if df is None or df.empty:
        raise ValueError("DataFrame vazio ou não carregado.")
    
    df = df[df['cnae_fiscal_principal'].isin(alvos)]

    if situacao is None:
        return df

    if isinstance(situacao, int):
        situacao = [situacao]

    # Filtra apenas situações válidas
    situacoes_validas = {2, 3, 4, 8}
    situacao_validada = [s for s in situacao if s in situacoes_validas]

    if not situacao_validada:
        raise ValueError("Código(s) de situação inválido(s). Use apenas 2, 3, 4 ou 8.")

    return df[df['situacao_cadastral'].isin(situacao_validada)]