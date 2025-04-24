import psycopg2 as pg
import pandas as pd
from sqlalchemy import create_engine


engine = create_engine("postgresql://postgres:root@localhost:5432/easyfood",connect_args={'client_encoding': 'utf8'})

vendas = pd.read_sql("SELECT * FROM public.venda",engine)
produto = pd.read_sql("SELECT * FROM public.produto",engine)
cliente = pd.read_sql("SELECT * FROM public.usuario",engine)
pedidos = pd.read_sql("SELECT * FROM public.pedido",engine)
itens_pedidos = pd.read_sql("SELECT * from public.itens_pedidos",engine)

vendas = vendas.astype(int)

itens_pedidos["valor_total"] = itens_pedidos["quantidade"] * itens_pedidos["valor"]

vendas.to_sql("vendas",con=engine,index=False, if_exists='replace', schema='bi')
produto.to_sql("produto",con=engine,index=False, if_exists='replace',schema='bi')
cliente.to_sql("cliente",con=engine,index=False, if_exists='replace',schema='bi')
pedidos.to_sql("pedidos",con=engine,index=False,if_exists='replace',schema='bi')
itens_pedidos.to_sql("itens_pedidos",con=engine, index=False, if_exists='replace',schema='bi')


# PostgreSQL (tabelas brutas) 
#     ↓
# Python com pandas (Transformações)
#     ↓
# Tabelas BI no PostgreSQL (via .to_sql)
#     ↓
# ODBC → Power BI