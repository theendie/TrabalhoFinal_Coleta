import pandas as pd
import sqlite3

arquivos = [
    'cassacao_cand_ES.csv',
    'cassacao_cand_MG.csv',
    'cassacao_cand_RJ.csv',
    'cassacao_cand_SP.csv'
]

# 1. Carregar e concatenar todos os CSVs em um único DataFrame
dfs = [pd.read_csv(arquivo, encoding='utf-8') for arquivo in arquivos]
dados_completos = pd.concat(dfs, ignore_index=True)

# 2. Conectar ao SQLite em memória
con = sqlite3.connect(':memory:')

# 3. Escrever o DataFrame consolidado no banco SQLite
dados_completos.to_sql('candidatos', con, index=False, if_exists='replace')

# Função para executar consultas SQL e exibir os resultados
def executar_query(query, descricao):
    print(f"\n{descricao}")
    resultado = pd.read_sql_query(query, con)
    print(resultado)


# Qual estado do Sudeste possui o maior número de candidatos cassados?
query1 = """
    SELECT 
        SG_UF AS estado, 
        COUNT(*) AS total_cassados
    FROM 
        candidatos
    WHERE 
        DS_MOTIVO LIKE '%abuso de poder%' 
        OR DS_MOTIVO LIKE '%ficha limpa%'
    GROUP BY 
        SG_UF
    ORDER BY 
        total_cassados DESC
    LIMIT 1;
"""
executar_query(query1, "Estado com mais candidatos cassados")

# Qual partido possui o maior número de candidatos com problemas legais?
query2 = """
    SELECT 
        SG_PARTIDO AS partido, 
        COUNT(*) AS total_cassados
    FROM 
        candidatos
    WHERE 
        DS_MOTIVO LIKE '%abuso de poder%' 
        OR DS_MOTIVO LIKE '%ficha limpa%'
    GROUP BY 
        SG_PARTIDO
    ORDER BY 
        total_cassados DESC
    LIMIT 1;
"""
executar_query(query2, "Partido com mais candidatos com problemas legais")

# Que padrão de perfil é mais comum entre candidatos cassados?
query3 = """
    SELECT 
        DS_GENERO AS genero, 
        DS_GRAU_INSTRUCAO AS grau_instrucao, 
        DS_CARGO AS cargo, 
        COUNT(*) AS total_cassados
    FROM 
        candidatos
    WHERE 
        DS_MOTIVO LIKE '%abuso de poder%' 
        OR DS_MOTIVO LIKE '%ficha limpa%'
    GROUP BY 
        DS_GENERO, DS_GRAU_INSTRUCAO, DS_CARGO
    ORDER BY 
        total_cassados DESC
    LIMIT 1;
"""
executar_query(query3, "Padrão de perfil mais comum entre candidatos cassados")

# Qual o motivo mais comum entre os candidatos com problemas legais?
query4 = """
    SELECT 
        DS_MOTIVO AS motivo, 
        COUNT(*) AS total_ocorrencias
    FROM 
        candidatos
    WHERE 
        DS_MOTIVO LIKE '%abuso de poder%' 
        OR DS_MOTIVO LIKE '%ficha limpa%'
    GROUP BY 
        DS_MOTIVO
    ORDER BY 
        total_ocorrencias DESC
    LIMIT 1;
"""
executar_query(query4, "Motivo mais comum entre candidatos com problemas legais")

# Quantos candidatos com problemas legais disputaram as eleições em cada turno?
query5 = """
    SELECT 
        DS_SIT_TOT_TURNO AS turno, 
        COUNT(*) AS total_cassados
    FROM 
        candidatos
    WHERE 
        DS_MOTIVO LIKE '%abuso de poder%' 
        OR DS_MOTIVO LIKE '%ficha limpa%'
    GROUP BY 
        DS_SIT_TOT_TURNO
    ORDER BY 
        total_cassados DESC;
"""
executar_query(query5, "Distribuição dos candidatos com problemas legais por turno")

# Quais são as ocupações mais comuns entre candidatos com problemas legais?
query6 = """
    SELECT 
        DS_OCUPACAO AS ocupacao, 
        COUNT(*) AS total_cassados
    FROM 
        candidatos
    WHERE 
        DS_MOTIVO LIKE '%abuso de poder%' 
        OR DS_MOTIVO LIKE '%ficha limpa%'
    GROUP BY 
        DS_OCUPACAO
    ORDER BY 
        total_cassados DESC
    LIMIT 5;
"""
executar_query(query6, "Ocupações mais comuns entre candidatos com problemas legais")




