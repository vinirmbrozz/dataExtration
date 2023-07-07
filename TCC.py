import psycopg2
import pandas as pd

def criarConexaoPostgresOrigem():
  try:
    conexao = psycopg2.connect(host='HOST', port= 'PORT', database='databaseOrig', user='USER', password= 'PASS')
    return conexao
  except (Exception, psycopg2.DatabaseError) as error:
    raise(error)


def criarConexaoPostgresDW():
  try:
    conexao = psycopg2.connect(host='HOST', port= 'PORT', database='databaseDest', user='USER', password= 'PASS')
    return conexao
  except (Exception, psycopg2.DatabaseError) as error:
    raise(error)

query = '''QUERY AQUI'''

def extracaoDPLCont(query, conexaoOrigem, conexaoDW):
  df = pd.read_sql(query, conexaoOrigem)
  nomeTabelaDW = 'dplcont'

  #Criar a tabela no DW
  df.to_sql(nomeTabelaDW, conexaoDW, if_exists='replace', index=False)

if __name__ == '__main__':
  conexaoOrigem = criarConexaoPostgresOrigem()
  conexaoDW = criarConexaoPostgresDW()
  extracaoDPLCont(query, conexaoOrigem, conexaoDW)
  print('Extração executada com sucesso.')

  conexaoDW.close()
  conexaoOrigem.close()
