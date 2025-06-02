# Desafio Engenheiro de Dados - Análise IDA Anatel

Este repositório contém a solução para o desafio de engenharia de dados proposto, que envolve a criação de um Data Mart para análise do Índice de Desempenho no Atendimento (IDA) da Anatel.

## Objetivo

O objetivo principal é construir um Data Mart em PostgreSQL, populado com dados do IDA para os serviços de Telefonia Celular (SMP), Telefonia Fixa (STFC) e Banda Larga Fixa (SCM), extraídos de arquivos ODS disponibilizados no Portal de Dados Abertos do Governo Federal. Adicionalmente, uma view é criada para calcular a taxa de variação mensal da "Taxa de Resolvidas em 5 dias úteis" e a diferença entre a taxa média e a taxa individual de cada grupo econômico.

A solução é entregue de forma containerizada utilizando Docker Compose.

## Estrutura do Repositório

```
.
├── docker-compose.yml        # Orquestra os containers da aplicação e do banco
├── etl_ida/                  # Diretório da aplicação Python ETL
│   ├── Dockerfile            # Define a imagem Docker para a aplicação ETL
│   ├── main_etl.py         # Script principal do processo ETL (inclui download com Selenium)
│   └── requirements.txt    # Dependências Python (inclui selenium)
├── sql_init/                 # Scripts SQL para inicialização do banco
│   ├── 01_create_tables.sql  # Cria as tabelas do Data Mart (fato e dimensões)
│   └── 02_create_view.sql    # Cria a view analítica solicitada
├── upload/                   # Diretório para colocar os arquivos ODS manualmente (fallback)
│   └── .gitkeep              # Placeholder para manter o diretório no Git
├── downloaded_ods/           # Diretório onde o Selenium tentará salvar os arquivos baixados
│   └── .gitkeep              # Placeholder
├── datamart_schema.md        # Documentação do modelo estrela do Data Mart
├── README.md                 # Este arquivo
└── .gitignore                # Arquivos e diretórios a serem ignorados pelo Git
```

## Tecnologias Utilizadas

*   **Linguagem:** Python 3.11
*   **Banco de Dados:** PostgreSQL 17.5
*   **Containerização:** Docker, Docker Compose
*   **Automação Web:** Selenium
*   **Bibliotecas Python Principais:** pandas, odfpy, psycopg2-binary, selenium, sqlalchemy

## Pré-requisitos

*   Docker instalado (https://docs.docker.com/get-docker/)
*   Docker Compose instalado (geralmente vem com o Docker Desktop)
*   Git instalado (https://git-scm.com/downloads)
*   **Microsoft Edge WebDriver:** Instalado e acessível no PATH do sistema OU o caminho para o executável `msedgedriver` deve ser fornecido através da variável de ambiente `EDGE_DRIVER_PATH`.
    *   Download: https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/
    *   Certifique-se de que a versão do WebDriver seja compatível com a versão do seu navegador Microsoft Edge.
*   **Opcional (Fallback):** Arquivos ODS do IDA Anatel (pelo menos um para cada serviço: SCM, SMP, STFC) colocados dentro do diretório `upload/` caso o download automático com Selenium falhe.
    *   Fonte original: https://dados.gov.br/dados/conjuntos-dados/indice-desempenho-atendimento

## Como Executar

1.  **Clone o repositório:**
    ```bash
    git clone <url-do-repositorio>
    cd <nome-do-diretorio-clonado>
    ```

2.  **Configure o WebDriver:**
    *   Certifique-se de que o `msedgedriver` está instalado e no PATH do seu sistema.
    *   **OU**, defina a variável de ambiente `EDGE_DRIVER_PATH` com o caminho completo para o executável do `msedgedriver`.
        *   Exemplo (Linux/macOS): `export EDGE_DRIVER_PATH="/path/to/your/msedgedriver"`
        *   Exemplo (Windows PowerShell): `$env:EDGE_DRIVER_PATH="C:\path\to\your\msedgedriver.exe"`

3.  **Execute o Docker Compose:**
    *   No terminal, na raiz do projeto (onde está o `docker-compose.yml`), execute:
        ```bash
        docker compose up --build
        ```
    *   O comando `--build` garante que a imagem Docker do ETL seja construída (ou reconstruída se houver alterações).
    *   Isso iniciará dois containers:
        *   `ida_postgres_db`: O container do banco de dados PostgreSQL.
        *   `ida_etl_app`: O container da aplicação Python que executará o ETL.
    *   O container do banco de dados executará automaticamente os scripts SQL em `sql_init/` para criar as tabelas e a view.
    *   O container do ETL iniciará e tentará:
        1.  **Download Automático:** Usar o Selenium e o Edge WebDriver (configurado no seu host) para navegar até o portal da Anatel e baixar os arquivos ODS definidos em `config.target_downloads` para o diretório `downloaded_ods/`.
        2.  **Leitura dos ODS:** Ler os arquivos ODS do diretório `downloaded_ods/`. Se este estiver vazio ou o download falhar, tentará ler do diretório `upload/` como fallback.
        3.  **Transformação:** Processar os dados lidos para o modelo estrela.
        4.  **Carga:** Carregar os dados transformados no banco de dados PostgreSQL.
    *   Acompanhe os logs no terminal para verificar o progresso do download, ETL e possíveis erros.

4.  **Verifique os Resultados:**
    *   Após a execução do ETL (verifique os logs do container `ida_etl_app`), você pode se conectar ao banco de dados PostgreSQL para verificar as tabelas e a view.
    *   Use uma ferramenta de cliente SQL (como DBeaver, pgAdmin, ou o comando `psql`) para conectar-se:
        *   **Host:** localhost
        *   **Porta:** 5432
        *   **Banco:** ida_datamart
        *   **Usuário:** user
        *   **Senha:** password
    *   Execute queries como:
        ```sql
        SELECT * FROM dim_tempo LIMIT 10;
        SELECT * FROM dim_grupo_economico;
        SELECT * FROM dim_servico;
        SELECT * FROM dim_metrica;
        SELECT COUNT(*) FROM fato_ida;
        SELECT * FROM v_taxa_variacao_resolvidas_5d;
        ```

5.  **Parar os Containers:**
    *   Pressione `Ctrl + C` no terminal onde o `docker compose up` está rodando.
    *   Para remover os containers e a rede (mas manter o volume de dados do banco, se desejar): `docker compose down`
    *   Para remover também o volume de dados: `docker compose down -v`

## Observações e Melhorias

*   **Execução do Selenium:** O script ETL (`main_etl.py`) agora tenta usar o Selenium para download automático. **Importante:** O WebDriver do Edge é executado na máquina host (onde você roda `docker compose up`), não dentro do container ETL. O script Python no container se comunica com o WebDriver na sua máquina. Certifique-se de que o WebDriver esteja corretamente instalado e configurado no host.
*   **Seletores Selenium:** Os seletores CSS e XPath usados no script para encontrar os botões de download podem precisar de ajustes se a estrutura do portal da Anatel mudar.
*   **Robustez do Download:** A espera pelo download é baseada em tempo (`time.sleep`). Uma abordagem mais robusta seria monitorar o diretório de downloads ou usar funcionalidades mais avançadas do Selenium/WebDriver para confirmar a conclusão do download.
*   **Tratamento de Erros:** O script ETL possui tratamento básico de erros e logging, mas pode ser aprimorado.
*   **Processamento Incremental:** A implementação atual realiza uma carga completa (ou append na fato). Para produção, lógica incremental seria mais adequada.
*   **Testes:** Adicionar testes unitários e de integração é recomendado.
*   **Segurança:** Credenciais do banco estão no `docker-compose.yml`. Usar secrets em produção.
*   **Pivot Dinâmico:** A view `v_taxa_variacao_resolvidas_5d` possui colunas pivotadas fixas. Uma solução mais dinâmica poderia ser implementada.

## Avaliação

*   **Docker Compose:** O projeto deve rodar completamente com `docker compose up --build` (assumindo que o WebDriver está configurado corretamente no host).
*   **Código SQL:** Clareza, organização e uso de `COMMENT ON`.
*   **Código Python:** Organização (OOP), clareza, documentação (pydoc), implementação do download automático com Selenium e aderência aos princípios de ETL.

