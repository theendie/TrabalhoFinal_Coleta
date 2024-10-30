import pandas as pd
import os


# Função para carregar um CSV e garantir leitura correta de acentos
def carregar_csv_com_erro(path):
    try:
        # Ler o CSV com codificação iso-8859-1 e delimitador correto
        df = pd.read_csv(path, encoding="iso-8859-1", sep=";", engine="python")
        print(f"Colunas carregadas de {path}: {df.columns.tolist()}")
        return df
    except UnicodeDecodeError:
        print(f"Erro de codificação ao ler {path}. Tentando com 'latin1'.")
        df = pd.read_csv(path, encoding="iso-8859-1", sep=";", engine="python")
        return df
    except Exception as e:
        print(f"Erro ao ler o arquivo {path}: {e}")
        return pd.DataFrame()  # Retorna DataFrame vazio em caso de erro


# Função para limpeza e padronização de cada par de arquivos
def limpar_csv(perfil_path, motivo_path):
    # Carregar dados com tratamento de codificação
    perfil = carregar_csv_com_erro(perfil_path)
    motivo = carregar_csv_com_erro(motivo_path)

    # Selecionar colunas relevantes no perfil
    perfil = perfil[
        [
            "SQ_CANDIDATO",
            "NM_CANDIDATO",
            "SG_PARTIDO",
            "DS_CARGO",
            "DS_GRAU_INSTRUCAO",
            "SG_UF",
            "DS_GENERO",
            "DS_COR_RACA",
            "DS_OCUPACAO",
            "DS_SIT_TOT_TURNO",
        ]
    ]

    # Selecionar colunas relevantes no motivo
    motivo = motivo[["SQ_CANDIDATO", "DS_TP_MOTIVO", "DS_MOTIVO"]]

    # Remover linhas com SQ_CANDIDATO nulo
    perfil.dropna(subset=["SQ_CANDIDATO"], inplace=True)
    motivo.dropna(subset=["SQ_CANDIDATO"], inplace=True)

    # Preencher valores ausentes com 'Desconhecido'
    motivo.fillna(
        {"DS_MOTIVO": "Desconhecido", "DS_TP_MOTIVO": "Desconhecido"}, inplace=True
    )

    # Padronizar códigos de exceção
    perfil.replace(
        {
            "DS_GENERO": {-1: "Não disponível", -3: "Não divulgado"},
            "DS_COR_RACA": {-1: "Não disponível", -3: "Não divulgado"},
        },
        inplace=True,
    )

    # Remover duplicatas
    perfil.drop_duplicates(subset="SQ_CANDIDATO", inplace=True)
    motivo.drop_duplicates(subset="SQ_CANDIDATO", inplace=True)

    # Unir as bases usando SQ_CANDIDATO
    df_merged = pd.merge(perfil, motivo, on="SQ_CANDIDATO", how="inner")

    return df_merged


# Caminhos das pastas
path_perfil = "dataset/consulta_cand_2022/"
path_motivo = "dataset/motivo_cassacao_2022/"

# Criar pasta 'dados_limpos' se não existir
os.makedirs("dados_limpos", exist_ok=True)

# Listar arquivos em cada pasta
arquivos_perfil = sorted(os.listdir(path_perfil))
arquivos_motivo = sorted(os.listdir(path_motivo))

# Iterar pelos arquivos e processá-los
for perfil_file, motivo_file in zip(arquivos_perfil, arquivos_motivo):
    perfil_path = os.path.join(path_perfil, perfil_file)
    motivo_path = os.path.join(path_motivo, motivo_file)

    # Limpeza e integração dos dados para cada par de arquivos
    df_limpo = limpar_csv(perfil_path, motivo_path)

    # Extrair nome do estado do arquivo (ex: 'consulta_cand_2022_ES.csv' -> 'ES')
    estado = perfil_file.split("_")[-1].split(".")[0]

    # Salvar o CSV limpo e integrado na pasta 'dados_limpos' com UTF-8
    output_path = f"dados_limpos/cassacao_cand_{estado}.csv"
    df_limpo.to_csv(output_path, index=False, encoding="iso-8859-1")

print("Processo de limpeza concluído")
