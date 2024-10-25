# processamento_de_vendas.sh - Gera um relatório de vendas
# Contato: www.linkedin.com/in/sarah-kolesnikova
# Autora: Sarah Kolesnikova
# ----------------------------------------------------------------------
# Histórico: 
#       v1.1 22/10/2024 Sarah Kolesnikova
#          - Inserir estrutura de decisão
#       v1.0 21/10/2024 Sarah Kolesnikova
#         - Inicio do Programa 
# Testado em:
#   bash 5.2.21
#
#-----------------------------------------------------------------------
#                   EXECUÇÃO
#
# acessar diretório raíz
cd /
# acessar diretório home
cd home
# acessar diretório ecommerce
cd ecommerce
if [ -d /vendas/ ]; then 
    cp dados_de_vendas.csv vendas
    cd vendas
    cp dados_de_vendas.csv backup
    cd backup
    mv dados_de_vendas.csv dados-$(date "+%Y%m%d").csv
    mv dados-$(date "+%Y%m%d").csv backup-dados-$(date "+%Y%m%d").csv
    touch data1.txt 
        echo "$(head -n 2 backup-dados-$(date "+%Y%m%d").csv)" > data1.txt
        echo "$(cut -d, -f 5 data1.txt)" > data1.txt
    touch dataf.txt 
        echo "$(tail -n 1 backup-dados-$(date "+%Y%m%d").csv)" > dataf.txt
        echo "$(cut -d, -f 5 dataf.txt)" > dataf.txt
    touch itens.txt
        echo "$(wc -l backup-dados-$(date "+%Y%m%d").csv)" > itens.txt
    # criar o arquivo relatorio.txt
    touch relatorio-$(date "+%Y%m%d").txt 
        # exibir frase:
        echo "Data do Relatório:" >> relatorio-$(date "+%Y%m%d").txt
        # inserir data do sistema operacional no arquivo criado
        echo $(date "+%Y/%m/%d %H:%M") >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase: 
        echo "Primeiro registro de vendas:" >> relatorio-$(date "+%Y%m%d").txt
        # inserir data do primeiro registro de vendas do arquivo backup-dados-yyyymmdd.csv
        cat data1.txt >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase: 
        echo "Último registro de vendas:" >> relatorio-$(date "+%Y%m%d").txt
        # inserir data do último registro de vendas do arquivo backup-dados-yyyymmdd.csv
        cat dataf.txt >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase:
        echo "Total de itens:" >> relatorio-$(date "+%Y%m%d").txt
        # quantidade total de itens diferentes vendidos
        cat itens.txt >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase:
        echo "10 PRIMEIRAS LINHAS:" >> relatorio-$(date "+%Y%m%d").txt
        # mostrar e incluir as 10 primeiras linhas do arquivo backup-dados-yyyymmdd.csv no relatório
        echo "$(head -n 11 backup-dados-$(date "+%Y%m%d").csv)" >> relatorio-$(date "+%Y%m%d").txt
    # removendo arquivos de extração de dados
    rm data1.txt
    rm dataf.txt
    rm itens.txt
    # compactando backup-dados-$(date "+%Y%m%d").csv em .zip no diretório backup 
    zip backup-dados-$(date "+%Y%m%d").zip backup-dados-$(date "+%Y%m%d").csv
    # apagando arquivo backup-dados-$(date "+%Y%m%d").csv do diretório backup
    rm backup-dados-$(date "+%Y%m%d").csv
    # apagando arquivo dados_de_vendas.csv do diretório vendas
    cd -
    rm dados_de_vendas.csv
    else 
    #criar subdiretório chamado "vendas" no diretório ecommerce
    mkdir vendas
    # copiar arquivo de base dados de ecommerce para o subdiretório vendas
    cp dados_de_vendas.csv vendas
    # acessar diretório vendas
    cd vendas
    # criar subdiretório chamado "backup" no diretório vendas
    mkdir backup

    # copiar arquivo de base dados de vendas para o subdiretório backup
    cp dados_de_vendas.csv backup
    # acessar diretório backup
    cd backup
    # renomear arquivo dados_de_vendas.csv no formato dados-yyyymmdd.csv no diretório backup
    mv dados_de_vendas.csv dados-$(date "+%Y%m%d").csv
    # renomear arquivo dados-yyyymmdd.csv no formato backup-dados-yyyymmdd.csv no diretório backup.
    mv dados-$(date "+%Y%m%d").csv backup-dados-$(date "+%Y%m%d").csv
    # extrair data do primeiro registro de vendas do arquivo backup-dados-yyyymmdd.csv
    touch data1.txt 
        echo "$(head -n 2 backup-dados-$(date "+%Y%m%d").csv)" > data1.txt
        echo "$(cut -d, -f 5 data1.txt)" > data1.txt

    # extrair data do último registro de vendas do arquivo backup-dados-yyyymmdd.csv
    touch dataf.txt 
        echo "$(tail -n 1 backup-dados-$(date "+%Y%m%d").csv)" > dataf.txt
        echo "$(cut -d, -f 5 dataf.txt)" > dataf.txt

    touch itens.txt
        echo "$(wc -l backup-dados-$(date "+%Y%m%d").csv)" > itens.txt
    # criar o arquivo relatorio.txt
    touch relatorio-$(date "+%Y%m%d").txt 
        # exibir frase:
        echo "Data do Relatório:" >> relatorio-$(date "+%Y%m%d").txt
        # inserir data do sistema operacional no arquivo criado
        echo $(date "+%Y/%m/%d %H:%M") >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase: 
        echo "Primeiro registro de vendas:" >> relatorio-$(date "+%Y%m%d").txt
        # inserir data do primeiro registro de vendas do arquivo backup-dados-yyyymmdd.csv
        cat data1.txt >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase: 
        echo "Último registro de vendas:" >> relatorio-$(date "+%Y%m%d").txt
        # inserir data do último registro de vendas do arquivo backup-dados-yyyymmdd.csv
        cat dataf.txt >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase:
        echo "Total de itens:" >> relatorio-$(date "+%Y%m%d").txt
        # quantidade total de itens diferentes vendidos
        cat itens.txt >> relatorio-$(date "+%Y%m%d").txt
        # exibir frase:
        echo "10 PRIMEIRAS LINHAS:" >> relatorio-$(date "+%Y%m%d").txt
        # mostrar e incluir as 10 primeiras linhas do arquivo backup-dados-yyyymmdd.csv no relatório
        echo "$(head -n 11 backup-dados-$(date "+%Y%m%d").csv)" >> relatorio-$(date "+%Y%m%d").txt
    # removendo arquivos de extração de dados
    rm data1.txt
    rm dataf.txt
    rm itens.txt
    # compactando backup-dados-$(date "+%Y%m%d").csv em .zip no diretório backup 
    zip backup-dados-$(date "+%Y%m%d").zip backup-dados-$(date "+%Y%m%d").csv
    # apagando arquivo backup-dados-$(date "+%Y%m%d").csv do diretório backup
    rm backup-dados-$(date "+%Y%m%d").csv
    # apagando arquivo dados_de_vendas.csv do diretório vendas
    cd -
    rm dados_de_vendas.csv
    fi