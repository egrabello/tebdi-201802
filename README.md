# TEBDI-201802
Trabalho Final do Curso de Tópicos Especiais em Bancos de Dados I - PESC/UFRJ - 2018/02

PROGRAMAS:
- loadHDFS.py --> Programa de carga de dados para o sistema HDFS
- sqlFuncs.py --> Funções auxiliares para a manipulação de consultas SQL (em string)
- web-api.py --> Programa que instancia um serviço web na porta 5000 do host local (http://localhost:5000) com a API de relatórios

ARQUIVOS DE CONFIGURAÇÃO:
- config.json --> Parâmetros gerais de configuração, utilizados pelos programas (.py)
- reports.json --> Lista / cadastro de relatórios utilizados na API Web (web-api.py)

VISUALIZAÇÃO (/dashboard):
- /dashboard/dashboard.pbix --> Painel construído à partir das APIs desenvolvidas
- /dashboard/apis.csv --> Arquivo CSV com a lista de APIs (ID e Nome)
- /dashboard/operations.csv --> Arquivo CSV com a lista de Operações (ID da API, ID e Nome da Operação)

EXTRAS:
- /notebooks --> Diretório com notebooks Jupyter utilizados para testes / desenvolvimento
