# Usa a imagem oficial do Airflow como base
FROM apache/airflow:3.0.6

# Copia o arquivo de dependências para dentro do container
COPY requirements.txt /requirements.txt

# Instala as bibliotecas listadas no requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
