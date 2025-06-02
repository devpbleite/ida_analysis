import os
import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Config:
    """Classe para gerenciar as configurações do ETL."""
    def __init__(self):
        """Inicializa as configurações."""
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        # Ajusta os paths para serem relativos ao diretório do script se rodando fora do compose
        # Ou usa paths absolutos se dentro do container
        container_env = os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"

        if container_env:
            self.ods_download_path = "/app/downloaded_ods" # Diretório para ODS baixados DENTRO do container
            self.ods_manual_path = "/home/ubuntu/upload" # Diretório com ODS fornecidos manualmente (montado via volume)
            self.processed_path = "/app/processed_ods" # Diretório para arquivos processados DENTRO do container
            self.webdriver_path = None # Assumir que webdriver não está no container ETL por padrão
        else:
            # Paths relativos para execução local
            self.ods_download_path = os.path.join(self.base_dir, "..", "downloaded_ods")
            self.ods_manual_path = os.path.join(self.base_dir, "..", "upload")
            self.processed_path = os.path.join(self.base_dir, "..", "processed_ods")
            # Caminho para o Edge WebDriver - AJUSTAR CONFORME NECESSÁRIO
            # Pode ser None se o webdriver estiver no PATH do sistema
            self.webdriver_path = os.getenv("EDGE_DRIVER_PATH", None)
            # Exemplo: self.webdriver_path = "/path/to/your/msedgedriver"

        # Cria os diretórios se não existirem
        os.makedirs(self.ods_download_path, exist_ok=True)
        os.makedirs(self.ods_manual_path, exist_ok=True)
        os.makedirs(self.processed_path, exist_ok=True)

        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = os.getenv("POSTGRES_PORT", "5432")
        self.db_name = os.getenv("POSTGRES_DB", "ida_datamart")
        self.db_user = os.getenv("POSTGRES_USER", "user")
        self.db_password = os.getenv("POSTGRES_PASSWORD", "password")
        self.anatel_data_url = "https://dados.gov.br/dados/conjuntos-dados/indice-desempenho-atendimento"
        # Serviços e anos alvo para download (ajustar conforme necessário)
        self.target_downloads = {
            "SCM": ["2019"], # Exemplo: Baixar SCM de 2019
            "SMP": ["2019"], # Exemplo: Baixar SMP de 2019
            "STFC": ["2019"] # Exemplo: Baixar STFC de 2019
        }
        self.service_mapping = {
            "SCM": "Banda Larga Fixa",
            "SMP": "Serviço Móvel Pessoal",
            "STFC": "Serviço Telefônico Fixo Comutado"
        }
        self.header_skip = 8
        self.download_wait_time = 30 # Segundos para esperar o download completar

class Extractor:
    """Classe responsável pela extração dos dados."""
    def __init__(self, config):
        """Inicializa o Extractor com as configurações."""
        self.config = config
        self.driver = None

    def _init_webdriver(self):
        """Inicializa o WebDriver do Edge."""
        logging.info("Inicializando o WebDriver do Edge...")
        options = EdgeOptions()
        options.use_chromium = True # Necessário para versões mais recentes
        # Configura o diretório de download
        prefs = {"download.default_directory": os.path.abspath(self.config.ods_download_path)}
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--headless") # Rodar em modo headless (sem interface gráfica)
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        try:
            if self.config.webdriver_path:
                service = EdgeService(executable_path=self.config.webdriver_path)
                self.driver = webdriver.Edge(service=service, options=options)
            else:
                # Tenta usar o webdriver do PATH
                self.driver = webdriver.Edge(options=options)
            logging.info("WebDriver inicializado com sucesso.")
            return True
        except Exception as e:
            logging.error(f"Falha ao inicializar o WebDriver: {e}")
            logging.error("Verifique se o Microsoft Edge WebDriver está instalado e acessível (no PATH ou via EDGE_DRIVER_PATH).")
            return False

    def _find_and_click_download_button(self, service, year):
        """Encontra e clica no botão de download para um serviço e ano específicos."""
        try:
            # Construir parte do texto esperado no título do recurso
            # Ex: "Índice de Desempenho no Atendimento - SCM - 2019"
            search_text_base = f"Índice de Desempenho no Atendimento - {service} - {year}"
            logging.info(f"Procurando por recurso contendo: {search_text_base}")

            # Localiza todos os itens de recurso
            resource_items = self.driver.find_elements(By.CSS_SELECTOR, "li.resource-item")
            if not resource_items:
                logging.warning("Nenhum item de recurso encontrado na página.")
                return False

            found_button = None
            for item in resource_items:
                try:
                    # Verifica se o título do recurso corresponde (pode precisar de ajuste no seletor)
                    title_element = item.find_element(By.CSS_SELECTOR, "h3.heading")
                    if search_text_base in title_element.text:
                        logging.info(f"Recurso encontrado: {title_element.text}")
                        # Encontra o botão "Acessar o recurso" dentro deste item
                        download_button = item.find_element(By.XPATH, ".//a[contains(@class, 'btn-primary') and contains(text(), 'Acessar o recurso')]")
                        found_button = download_button
                        break # Para após encontrar o primeiro correspondente
                except NoSuchElementException:
                    continue # Continua para o próximo item se não encontrar título ou botão

            if found_button:
                logging.info(f"Clicando no botão de download para {service} {year}...")
                # Scroll até o botão para garantir visibilidade (opcional, mas pode ajudar)
                self.driver.execute_script("arguments[0].scrollIntoView(true);", found_button)
                time.sleep(1) # Pequena pausa
                found_button.click()
                logging.info(f"Botão clicado. Aguardando download...")
                # Espera um tempo fixo para o download (pode ser melhorado)
                time.sleep(self.config.download_wait_time)
                return True
            else:
                logging.warning(f"Botão de download para {service} {year} não encontrado.")
                return False

        except Exception as e:
            logging.error(f"Erro ao tentar encontrar/clicar no botão de download para {service} {year}: {e}")
            return False

    def download_data(self):
        """Baixa os arquivos ODS do portal da Anatel usando Selenium."""
        if not self._init_webdriver():
            return False # Falha ao iniciar o webdriver

        success_count = 0
        total_targets = sum(len(years) for years in self.config.target_downloads.values())

        try:
            logging.info(f"Navegando para: {self.config.anatel_data_url}")
            self.driver.get(self.config.anatel_data_url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.resource-item"))
            )
            logging.info("Página carregada.")

            # Tenta expandir a seção "Recursos" se estiver colapsada (ajustar seletor se necessário)
            try:
                recursos_header = self.driver.find_element(By.XPATH, "//button[contains(., 'Recursos')]")
                # Verifica se está colapsado (exemplo, pode variar)
                if "collapsed" in recursos_header.get_attribute("class"):
                    logging.info("Expandindo seção Recursos...")
                    recursos_header.click()
                    time.sleep(2) # Espera a expansão
            except NoSuchElementException:
                logging.warning("Não foi possível encontrar o botão para expandir Recursos, assumindo que já está expandido.")
            except Exception as e:
                 logging.warning(f"Erro ao tentar expandir Recursos: {e}")

            # Itera sobre os alvos definidos na configuração
            for service, years in self.config.target_downloads.items():
                for year in years:
                    logging.info(f"Tentando baixar dados para {service} do ano {year}...")
                    if self._find_and_click_download_button(service, year):
                        success_count += 1
                    else:
                        logging.warning(f"Falha ao baixar {service} {year}.")

        except TimeoutException:
            logging.error("Tempo esgotado esperando a página carregar ou elementos aparecerem.")
        except Exception as e:
            logging.error(f"Ocorreu um erro durante o processo de download: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                logging.info("WebDriver fechado.")

        logging.info(f"Tentativa de download concluída. {success_count} de {total_targets} arquivos alvo foram baixados (verifique o diretório {self.config.ods_download_path}).")
        return success_count > 0 # Retorna True se pelo menos um download foi tentado com sucesso

    def read_ods_files(self, directory):
        """Lê os arquivos ODS de um diretório especificado."""
        all_data = {}
        logging.info(f"Lendo arquivos ODS do diretório: {directory}")
        try:
            if not os.path.exists(directory) or not os.listdir(directory):
                logging.warning(f"Diretório {directory} está vazio ou não existe.")
                return {}

            for filename in os.listdir(directory):
                if filename.endswith(".ods") and not filename.startswith(".~"): # Ignora arquivos temporários
                    file_path = os.path.join(directory, filename)
                    service_type = "UNKNOWN"
                    # Tenta extrair o tipo de serviço do nome do arquivo
                    for key in self.config.service_mapping.keys():
                        if key in filename.upper():
                            service_type = key
                            break

                    if service_type != "UNKNOWN":
                        logging.info(f"Lendo arquivo {filename} para o serviço {service_type}...")
                        try:
                            df = pd.read_excel(file_path, engine="odf", header=self.config.header_skip)
                            df["servico_sigla"] = service_type
                            df["arquivo_origem"] = filename
                            if service_type not in all_data:
                                all_data[service_type] = []
                            all_data[service_type].append(df)
                            logging.info(f"Arquivo {filename} lido com sucesso.")
                        except Exception as e:
                            logging.error(f"Falha ao ler o arquivo {filename}: {e}")
                    else:
                        logging.warning(f"Ignorando arquivo com nome não reconhecido: {filename}")

            final_data = {}
            for service, dfs in all_data.items():
                if dfs:
                    final_data[service] = pd.concat(dfs, ignore_index=True)
            return final_data

        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado ao ler os arquivos ODS: {e}")
            return {}

class Transformer:
    """Classe responsável pela transformação dos dados."""
    def __init__(self, config):
        """Inicializa o Transformer."""
        self.config = config

    def transform_data(self, raw_data_dict):
        """Transforma os dados brutos lidos dos ODS para o formato do Data Mart."""
        logging.info("Iniciando transformação dos dados...")
        if not raw_data_dict:
            logging.error("Nenhum dado bruto para transformar.")
            return None

        all_transformed_dfs = []

        for service_type, df_raw in raw_data_dict.items(): # Agora é um DF único por serviço
            logging.info(f"Transformando dados para o serviço {service_type}...")

            # Verifica se as colunas esperadas existem
            if df_raw.empty or len(df_raw.columns) < 2:
                logging.warning(f"DataFrame para {service_type} está vazio ou tem poucas colunas. Pulando.")
                continue

            # Renomeia as duas primeiras colunas de forma robusta
            col_grupo = df_raw.columns[0]
            col_metrica = df_raw.columns[1]
            df_raw = df_raw.rename(columns={col_grupo: "grupo_economico", col_metrica: "metrica_nome"})

            # Remove linhas onde grupo ou métrica são nulos
            df_raw = df_raw.dropna(subset=["grupo_economico", "metrica_nome"])
            # Remove linhas de cabeçalho repetidas (se houver)
            df_raw = df_raw[~df_raw["grupo_economico"].astype(str).str.contains("GRUPO ECONÔMICO", na=False)]

            # Identifica colunas de data (formato YYYY-MM ou YYYY/MM ou Timestamps)
            date_cols = []
            for col in df_raw.columns:
                # CORREÇÃO: Verifica o tipo ANTES de tentar operações de string/indexação
                if isinstance(col, pd.Timestamp):
                    date_cols.append(col)
                elif isinstance(col, str):
                    # Tenta identificar padrões como YYYY-MM ou YYYY/MM
                    if len(col) == 7 and col[4] in ["-", "/"] and col[:4].isdigit() and col[5:].isdigit():
                        date_cols.append(col)
                    # Adicionar outras lógicas de identificação de string de data se necessário

            if not date_cols:
                logging.error(f"Não foi possível encontrar colunas de data para o serviço {service_type}. Colunas: {df_raw.columns}")
                continue
            logging.info(f"Colunas de data identificadas para {service_type}: {date_cols}")

            # Unpivot (melt)
            id_vars = ["grupo_economico", "metrica_nome", "servico_sigla", "arquivo_origem"]
            df_melted = df_raw.melt(
                id_vars=id_vars,
                value_vars=date_cols,
                var_name="ano_mes_raw",
                value_name="valor"
            )

            # Limpar e formatar coluna ano_mes
            df_melted["ano_mes"] = pd.to_datetime(df_melted["ano_mes_raw"], errors="coerce").dt.strftime("%Y-%m")
            df_melted = df_melted.dropna(subset=["ano_mes"]) # Remove linhas onde a data não pôde ser convertida

            # Extrair ano e mês
            df_melted["ano"] = pd.to_datetime(df_melted["ano_mes"]).dt.year
            df_melted["mes"] = pd.to_datetime(df_melted["ano_mes"]).dt.month

            # Limpar coluna valor
            df_melted["valor"] = pd.to_numeric(df_melted["valor"], errors="coerce")

            # Selecionar colunas finais
            df_transformed = df_melted[[
                "ano", "mes", "ano_mes", "grupo_economico", "servico_sigla", "metrica_nome", "valor"
            ]].copy()

            # Tratar nomes de métricas/grupos (remover espaços extras, etc.)
            df_transformed["grupo_economico"] = df_transformed["grupo_economico"].astype(str).str.strip()
            df_transformed["metrica_nome"] = df_transformed["metrica_nome"].astype(str).str.strip()

            all_transformed_dfs.append(df_transformed)

        if not all_transformed_dfs:
            logging.error("Nenhuma informação foi transformada com sucesso.")
            return None

        df_final = pd.concat(all_transformed_dfs, ignore_index=True)

        # Criar DataFrames para as dimensões
        dim_tempo = df_final[["ano", "mes", "ano_mes"]].drop_duplicates().reset_index(drop=True)
        dim_grupo = pd.DataFrame({"nome": df_final["grupo_economico"].unique()})
        dim_servico = pd.DataFrame(self.config.service_mapping.items(), columns=["sigla", "nome"])
        dim_metrica = pd.DataFrame({"nome": df_final["metrica_nome"].unique()})

        # Criar DataFrame para a tabela Fato (ainda sem IDs)
        fato_ida = df_final[["ano_mes", "grupo_economico", "servico_sigla", "metrica_nome", "valor"]]

        logging.info("Transformação dos dados concluída.")
        return dim_tempo, dim_grupo, dim_servico, dim_metrica, fato_ida

class Loader:
    """Classe responsável pelo carregamento dos dados no Data Mart."""
    def __init__(self, config):
        """Inicializa o Loader."""
        self.config = config
        self.conn = None
        self.engine = None 

    def connect_db(self):
        """Estabelece conexão com o banco de dados PostgreSQL."""
        try:
            # Importar dentro do método para evitar erro se não instalado globalmente
            import psycopg2
            from sqlalchemy import create_engine, text

            db_url = f"postgresql+psycopg2://{self.config.db_user}:{self.config.db_password}@{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
            self.engine = create_engine(db_url)
            self.conn = self.engine.connect()
            logging.info("Conexão com PostgreSQL estabelecida via SQLAlchemy.")
            return True
        except ImportError:
            logging.error("Bibliotecas psycopg2 ou SQLAlchemy não encontradas. Instale-as: pip install psycopg2-binary sqlalchemy")
            return False
        except Exception as e:
            logging.error(f"Falha ao conectar ao PostgreSQL: {e}")
            self.conn = None
            self.engine = None
            return False

    def _get_or_insert_dimension(self, df_dim, table_name, key_col, value_col):
        """Insere dados na dimensão se não existirem e retorna mapeamento Valor -> ID."""
        logging.info(f"Processando dimensão: {table_name}")
        # Carrega dados existentes da dimensão
        try:
            existing_df = pd.read_sql(f"SELECT {key_col}, {value_col} FROM {table_name}", self.conn)
            existing_map = pd.Series(existing_df[key_col].values, index=existing_df[value_col]).to_dict()
        except Exception as e:
            logging.warning(f"Não foi possível carregar dados existentes de {table_name} (pode ser a primeira execução): {e}")
            existing_map = {}

        # Identifica novos valores
        new_values = df_dim[~df_dim[value_col].isin(existing_map.keys())]

        # Insere novos valores
        if not new_values.empty:
            try:
                logging.info(f"Inserindo {len(new_values)} novos registros em {table_name}...")
                # Remove a coluna chave se ela existir no DataFrame (geralmente não existe aqui)
                if key_col in new_values.columns:
                     new_values = new_values.drop(columns=[key_col])
                new_values.to_sql(table_name, self.engine, if_exists="append", index=False)
                # Recarrega o mapeamento completo após inserção
                existing_df = pd.read_sql(f"SELECT {key_col}, {value_col} FROM {table_name}", self.conn)
                existing_map = pd.Series(existing_df[key_col].values, index=existing_df[value_col]).to_dict()
            except Exception as e:
                logging.error(f"Erro ao inserir novos dados em {table_name}: {e}")
                # Retorna o mapa existente antes da tentativa de inserção
                return existing_map

        return existing_map

    def load_data(self, dims_and_fact):
        """Carrega todas as dimensões e a tabela fato."""
        if not self.conn:
            logging.error("Sem conexão com o banco de dados.")
            return

        dim_tempo, dim_grupo, dim_servico, dim_metrica, fato_ida_no_ids = dims_and_fact

        try:
            # Importar text aqui também para garantir disponibilidade
            from sqlalchemy import text

            # Carregar/Obter IDs das dimensões
            map_tempo = self._get_or_insert_dimension(dim_tempo, "dim_tempo", "id_tempo", "ano_mes")
            map_grupo = self._get_or_insert_dimension(dim_grupo, "dim_grupo_economico", "id_grupo", "nome")
            map_servico = self._get_or_insert_dimension(dim_servico, "dim_servico", "id_servico", "sigla")
            map_metrica = self._get_or_insert_dimension(dim_metrica, "dim_metrica", "id_metrica", "nome")

            # Mapear IDs na tabela Fato
            logging.info("Mapeando IDs na tabela Fato...")
            fato_ida_final = fato_ida_no_ids.copy()
            fato_ida_final["id_tempo"] = fato_ida_final["ano_mes"].map(map_tempo)
            fato_ida_final["id_grupo"] = fato_ida_final["grupo_economico"].map(map_grupo)
            fato_ida_final["id_servico"] = fato_ida_final["servico_sigla"].map(map_servico)
            fato_ida_final["id_metrica"] = fato_ida_final["metrica_nome"].map(map_metrica)

            # Selecionar colunas finais para a fato_ida
            fato_ida_final = fato_ida_final[["id_tempo", "id_grupo", "id_servico", "id_metrica", "valor"]]

            # Remover linhas com IDs nulos 
            original_rows = len(fato_ida_final)
            fato_ida_final = fato_ida_final.dropna(subset=["id_tempo", "id_grupo", "id_servico", "id_metrica"])
            if len(fato_ida_final) < original_rows:
                logging.warning(f"{original_rows - len(fato_ida_final)} linhas da tabela fato foram removidas devido a IDs de dimensão não encontrados.")

            # Carregar tabela Fato usando TRUNCATE + INSERT
            if not fato_ida_final.empty:
                logging.info(f"Carregando {len(fato_ida_final)} registros na tabela fato_ida (TRUNCATE + INSERT)...")
                with self.conn.begin(): # Inicia transação
                    # Limpa a tabela fato antes de inserir
                    self.conn.execute(text("TRUNCATE TABLE fato_ida RESTART IDENTITY;"))
                    logging.info("Tabela fato_ida limpa (TRUNCATE).")
                    # Insere os novos dados
                    fato_ida_final.to_sql("fato_ida", self.engine, if_exists="append", index=False)
                logging.info("Carga da tabela fato concluída.")
            else:
                logging.warning("Nenhum dado válido para carregar na tabela fato.")

        except Exception as e:
            logging.error(f"Erro durante o carregamento dos dados: {e}")
            # Considerar rollback se estiver em transação

    def close_db(self):
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.conn.close()
            logging.info("Conexão com PostgreSQL fechada.")
        if self.engine:
            self.engine.dispose()

class ETLOrchestrator:
    """Orquestra o fluxo completo do ETL."""
    def __init__(self):
        """Inicializa o orquestrador."""
        self.config = Config()
        self.extractor = Extractor(self.config)
        self.transformer = Transformer(self.config)
        self.loader = Loader(self.config)

    def run_etl(self):
        """Executa o processo ETL completo."""
        logging.info("===========================================")
        logging.info("Iniciando processo ETL IDA Anatel...")
        logging.info("===========================================")

        # 1. Extração
        logging.info("--- Fase de Extração ---")
        # Tenta download automático
        download_success = self.extractor.download_data()

        # Decide qual diretório ler
        read_directory = None
        if download_success and os.path.exists(self.config.ods_download_path) and os.listdir(self.config.ods_download_path):
            logging.info("Usando arquivos ODS do diretório de download automático.")
            read_directory = self.config.ods_download_path
        elif os.path.exists(self.config.ods_manual_path) and os.listdir(self.config.ods_manual_path):
            logging.info("Download automático falhou ou diretório vazio. Usando arquivos ODS do diretório manual.")
            read_directory = self.config.ods_manual_path
        else:
            logging.error("Nenhum arquivo ODS encontrado nos diretórios de download ou manual.")
            return

        raw_data = self.extractor.read_ods_files(read_directory)
        if not raw_data:
             logging.error("Falha na extração dos dados. Abortando ETL.")
             return
        logging.info("Extração concluída.")

        # 2. Transformação
        logging.info("--- Fase de Transformação ---")
        transformed_data = self.transformer.transform_data(raw_data)
        if transformed_data is None:
            logging.error("Falha na transformação dos dados. Abortando ETL.")
            return
        logging.info("Transformação concluída.")

        # 3. Carga
        logging.info("--- Fase de Carga ---")
        if self.loader.connect_db():
            self.loader.load_data(transformed_data)
            self.loader.close_db()
        else:
            logging.error("Não foi possível conectar ao banco de dados para carregar os dados.")

        logging.info("===========================================")
        logging.info("Processo ETL concluído.")
        logging.info("===========================================")

# Bloco principal de execução
if __name__ == "__main__":
    # Adiciona um pequeno delay para dar tempo ao DB iniciar no docker-compose
    startup_delay = int(os.getenv("ETL_STARTUP_DELAY", "5"))
    logging.info(f"Aguardando {startup_delay} segundos para o banco de dados iniciar...")
    time.sleep(startup_delay)

    # Cria e executa o orquestrador
    orchestrator = ETLOrchestrator()
    orchestrator.run_etl()

