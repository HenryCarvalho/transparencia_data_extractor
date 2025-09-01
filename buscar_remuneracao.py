import requests
import pandas as pd
import time
import re
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
#from requests.packages.urllib3.util.retry import Retry

# === FUNÇÃO PARA LER CHAVE API DO ARQUIVO ===
def ler_chave_api(nome_arquivo='chave_api.txt'):
    try:
        with open(nome_arquivo, 'r', encoding='utf-8') as arquivo:
            chave = arquivo.read().strip()
            
            if not chave:
                raise ValueError("Arquivo de chave API está vazio")
            
            print("✅ Chave API carregada com sucesso")
            return chave
            
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo {nome_arquivo} não encontrado.")
        print("💡 Crie um arquivo 'chave_api.txt' com sua chave API")
        exit()
    except Exception as e:
        print(f"❌ Erro ao ler chave API: {e}")
        exit()


# === CONFIGURAÇÃO ===
API_KEY = ler_chave_api('chave_api.txt')  # Lê a chave do arquivo
URL_REMUNERACAO = 'https://api.portaldatransparencia.gov.br/api-de-dados/servidores/remuneracao'

headers = {
    'Accept': 'application/json',
    'chave-api-dados': API_KEY,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# === FUNÇÃO PARA LER CPFs DO ARQUIVO ===
def ler_cpfs_do_arquivo(nome_arquivo='cpf.txt'):
    """
    Lê CPFs de um arquivo de texto (formato: XXX.XXX.XXX-XX)
    e retorna lista de CPFs apenas com dígitos
    """
    cpfs_limpos = []
    
    try:
        with open(nome_arquivo, 'r', encoding='utf-8') as arquivo:
            linhas = arquivo.readlines()
            
            for numero_linha, linha in enumerate(linhas, 1):
                linha = linha.strip()
                
                # Ignora linhas vazias e comentários
                if not linha or linha.startswith('#'):
                    continue
                
                # Remove formatação (pontos e traços)
                cpf_limpo = re.sub(r'[^\d]', '', linha)
                
                # Verifica se tem 11 dígitos
                if len(cpf_limpo) == 11:
                    cpfs_limpos.append(cpf_limpo)
                else:
                    print(f"CPF inválido na linha {numero_linha}: {linha}")
    
    except FileNotFoundError:
        print(f"Erro: Arquivo {nome_arquivo} não encontrado.")
        return []
    except Exception as e:
        print(f"Erro ao ler arquivo: {e}")
        return []
    
    return cpfs_limpos

# === CONFIGURAR SESSÃO COM RETRY ===
def criar_sessao_com_retry():
    """Cria uma sessão HTTP com política de retry"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# === FUNÇÃO PARA BUSCAR REMUNERAÇÃO ===
def buscar_remuneracao_por_cpfs(cpfs, mes_ano='202506'):
    """
    Busca remuneração para uma lista de CPFs
    """
    session = criar_sessao_com_retry()
    todas_remuneracoes = []
    cpfs_com_erro = []
    cpfs_sem_dados = []
    
    print(f"Iniciando consulta de {len(cpfs)} CPFs...")
    
    for i, cpf in enumerate(cpfs, 1):
        print(f"Processando CPF {i}/{len(cpfs)}: {cpf}")
        
        params = {
            'cpf': cpf,
            'mesAno': mes_ano,  # Formato AAAAMM
            'pagina': 1
        }
        
        try:
            response = session.get(URL_REMUNERACAO, headers=headers, 
                                 params=params, timeout=30)
            
            if response.status_code == 200:
                dados = response.json()
                if dados:
                    # Adiciona CPF aos dados para referência
                    for item in dados:
                        item['cpf_consulta'] = cpf
                        item['mes_ano_consulta'] = mes_ano
                    todas_remuneracoes.extend(dados)
                    print(f"  Dados encontrados")
                else:
                    print(f"  ⓘ Nenhum dado encontrado")
                    cpfs_sem_dados.append(cpf)
            
            elif response.status_code == 400:
                print(f"  Erro 400: Parâmetros inválidos para CPF {cpf}")
                cpfs_com_erro.append(cpf)
            
            elif response.status_code == 403:
                print(f"  Erro 403: Acesso negado. Verifique a chave API.")
                break
            
            elif response.status_code == 429:
                print("  ⚠ Erro 429: Muitas requisições. Aguardando 5 segundos...")
                time.sleep(5)
                continue
            
            else:
                print(f"  Erro HTTP {response.status_code} para CPF {cpf}")
                cpfs_com_erro.append(cpf)
                
        except requests.exceptions.RequestException as e:
            print(f"  Erro de conexão: {e}")
            cpfs_com_erro.append(cpf)
        
        # Respeitar limite de requisições da API (90 req/min)
        time.sleep(0.67)
    
    return todas_remuneracoes, cpfs_com_erro, cpfs_sem_dados

# === FUNÇÃO PARA PROCESSAR DADOS ===
def processar_dados_remuneracao(dados_remuneracao):
    """
    Processa os dados brutos da API para um formato estruturado
    """
    linhas_processadas = []
    
    for dados in dados_remuneracao:
        servidor = dados.get('servidor', {})
        pessoa = servidor.get('pessoa', {})
        orgao_lotacao = servidor.get('orgaoServidorLotacao', {})
        
        # Processar cada remuneraçãoDTO
        for remuneracao in dados.get('remuneracoesDTO', []):
            linha = {
                'cpf_consulta': dados.get('cpf_consulta'),
                'mes_consulta': dados.get('mes_ano_consulta'),
                'nome': pessoa.get('nome', 'N/A'),
                'cpf_formatado': pessoa.get('cpfFormatado', 'N/A'),
                'orgao_lotacao_codigo': orgao_lotacao.get('codigo', 'N/A'),
                'orgao_lotacao_nome': orgao_lotacao.get('nome', 'N/A'),
                'situacao': servidor.get('situacao', 'N/A'),
                'cargo': servidor.get('funcao', {}).get('descricaoFuncaoCargo', 'N/A'),
                'mes_ano': remuneracao.get('mesAno', 'N/A'),
                'remuneracao_total': remuneracao.get('valorTotalRemuneracaoAposDeducoes', '0,00'),
                'remuneracao_bruta': remuneracao.get('remuneracaoBasicaBruta', '0,00'),
                'verbas_indenizatorias': remuneracao.get('verbasIndenizatorias', '0,00'),
                'imposto_retido_fonte': remuneracao.get('impostoRetidoNaFonte', '0,00'),
                'previdencia_oficial': remuneracao.get('previdenciaOficial', '0,00')
            }
            linhas_processadas.append(linha)
    
    return linhas_processadas

# === FUNÇÃO PARA EXPORTAR DADOS ===
def exportar_dados(dados_processados, nome_arquivo='remuneracao_cpfs.xlsx'):
    """
    Exporta dados para Excel
    """
    if not dados_processados:
        print("Nenhum dado para exportar.")
        return False
    
    try:
        df = pd.DataFrame(dados_processados)
        
        with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Remuneracao', index=False)
            
            # Autoajustar largura das colunas
            worksheet = writer.sheets['Remuneracao']
            for idx, column in enumerate(df.columns):
                max_length = max(df[column].astype(str).map(len).max(), len(column))
                worksheet.column_dimensions[chr(65 + idx)].width = max_length + 2
        
        print(f"Dados exportados para: {nome_arquivo}")
        print(f"Total de registros: {len(df)}")
        return True
        
    except Exception as e:
        print(f"Erro ao exportar para Excel: {e}")
        return False

# === FUNÇÃO PRINCIPAL ===
def main():
    """Função principal do script"""
    print("=" * 60)
    print("CONSULTA DE REMUNERAÇÃO - PORTAL DA TRANSPARÊNCIA")
    print("=" * 60)
    
    # Ler CPFs do arquivo
    cpfs = ler_cpfs_do_arquivo('cpf.txt')
    
    if not cpfs:
        print("Nenhum CPF válido encontrado. Verifique o arquivo cpf.txt")
        return
    
    print(f"{len(cpfs)} CPFs válidos encontrados no arquivo")
    
    # Definir mês/ano da consulta (formato AAAAMM)
    mes_ano = input("Digite o mês/ano para consulta (AAAAMM) [202506]: ").strip()
    if not mes_ano:
        mes_ano = '202506'
    
    # Buscar remunerações
    dados_brutos, cpfs_com_erro, cpfs_sem_dados = buscar_remuneracao_por_cpfs(cpfs, mes_ano)
    
    # Processar dados
    if dados_brutos:
        dados_processados = processar_dados_remuneracao(dados_brutos)
        
        # Exportar dados
        nome_arquivo = f"remuneracao_{mes_ano}.xlsx"
        exportar_dados(dados_processados, nome_arquivo)
        
        # Salvar dados brutos em JSON
        try:
            import json
            json_file = f"dados_brutos_{mes_ano}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(dados_brutos, f, ensure_ascii=False, indent=2)
            print(f"Dados brutos salvos em: {json_file}")
        except Exception as e:
            print(f"⚠ Não foi possível salvar JSON: {e}")
    else:
        print("Nenhum dado de remuneração encontrado.")
    
    # Salvar relatórios
    if cpfs_com_erro:
        with open('cpfs_com_erro.txt', 'w', encoding='utf-8') as f:
            f.write("CPFs com erro na consulta:\n")
            for cpf in cpfs_com_erro:
                f.write(f"{cpf}\n")
        print(f"CPFs com erro: {len(cpfs_com_erro)}")
    
    if cpfs_sem_dados:
        with open('cpfs_sem_dados.txt', 'w', encoding='utf-8') as f:
            f.write("CPFs sem dados encontrados:\n")
            for cpf in cpfs_sem_dados:
                f.write(f"{cpf}\n")
        print(f"CPFs sem dados: {len(cpfs_sem_dados)}")

# === EXECUÇÃO ===
if __name__ == "__main__":
    main()