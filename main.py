import os
import pandas as pd
import time
from tqdm import tqdm

# Caminho do arquivo
path_data = r'.\microdados_enem_2023\DADOS\ITENS_PROVA_2023.csv'

start_time = time.time()

try:
    if not os.path.exists(path_data):
        raise FileNotFoundError(f"Arquivo não encontrado: {path_data}")
    
    print("Carregando dados...")

    # Definir chunksize e codificação fixa
    chunksize = 1000  # Número de linhas por chunk
    encoding = 'latin1'  # Alternativamente, teste 'utf-8' ou 'ISO-8859-1'

    # Criar uma lista para armazenar os chunks
    data_chunks = []
    
    # Iterar sobre os chunks e exibir a barra de progresso
    with pd.read_csv(path_data, sep=';', encoding=encoding, chunksize=chunksize) as reader:
        for chunk in tqdm(reader, desc="Carregando", unit="chunk"):
            data_chunks.append(chunk)
    
    # Concatenar todos os chunks em um único DataFrame
    dados_df = pd.concat(data_chunks, ignore_index=True)
    
    print("Dados carregados com sucesso.")
    print(dados_df.head())  # Exibir as primeiras linhas do DataFrame

except Exception as e:
    print(f"Erro: {e}")

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Tempo de carregamento: {elapsed_time:.2f} segundos.")
