# Dockerfile para a aplicação ETL IDA Anatel

# Imagem base Python especificada
FROM python:3.11.12-bookworm

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia o arquivo de dependências
COPY requirements.txt .

# Instala as dependências
# --no-cache-dir para não armazenar cache e diminuir o tamanho da imagem
# --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org para evitar problemas de SSL em alguns ambientes
RUN pip install --no-cache-dir --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# Copia o script principal do ETL para o diretório de trabalho
COPY main_etl.py .

# Define o comando padrão para executar o script ETL quando o container iniciar
CMD ["python", "main_etl.py"]

