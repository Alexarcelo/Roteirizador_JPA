import streamlit as st
import mysql.connector
import decimal
import pandas as pd
from datetime import date, time
from google.oauth2 import service_account
import gspread 
from itertools import combinations
import math
import requests
from st_aggrid import AgGrid, GridOptionsBuilder

def gerar_df_phoenix(vw_name, base_luck):

    data_hoje = date.today()

    data_hoje_str = data_hoje.strftime("%Y-%m-%d")
    
    config = {'user': 'user_automation_jpa', 'password': 'luck_jpa_2024', 'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com', 'database': base_luck}

    conexao = mysql.connector.connect(**config)

    cursor = conexao.cursor()

    if vw_name=='vw_router':

        request_name = f'SELECT * FROM {vw_name} WHERE {vw_name}.`Data Execucao`>={data_hoje_str}'

    else:

        request_name = f'SELECT * FROM {vw_name}'

    cursor.execute(request_name)

    resultado = cursor.fetchall()
    
    cabecalho = [desc[0] for desc in cursor.description]

    cursor.close()

    conexao.close()

    df = pd.DataFrame(resultado, columns=cabecalho)

    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

    return df

def puxar_dados_phoenix():

    st.session_state.df_router = gerar_df_phoenix('vw_router', st.session_state.base_luck)

    st.session_state.df_router = st.session_state.df_router[(st.session_state.df_router['Status da Reserva']!='CANCELADO')].reset_index(drop=True)

    st.session_state.df_veiculos = gerar_df_phoenix('vw_veiculos', st.session_state.base_luck)

    st.session_state.df_veiculos = st.session_state.df_veiculos.rename(columns={'name': 'Veiculo'})

    st.session_state.df_motoristas = gerar_df_phoenix('vw_motoristas', 'test_phoenix_joao_pessoa')

    st.session_state.df_motoristas = st.session_state.df_motoristas.rename(columns={'nickname': 'Motorista'})

    st.session_state.df_guias = gerar_df_phoenix('vw_guias', 'test_phoenix_joao_pessoa')

    st.session_state.df_guias = st.session_state.df_guias.rename(columns={'nickname': 'Guia'})

def puxar_sequencias_hoteis(id_gsheet, dict_abas_df_hoteis):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)

    for key, value in dict_abas_df_hoteis.items():

        aba = key

        df_hotel = value
        
        sheet = spreadsheet.worksheet(aba)

        sheet_data = sheet.get_all_values()

        st.session_state[df_hotel] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

        for coluna in ['Hoteis Juntos p/ Apoios', 'Hoteis Juntos p/ Carro Principal']:

            st.session_state[df_hotel][coluna] = st.session_state[df_hotel][coluna].apply(lambda x: None if pd.isna(x) or str(x).strip() == '' else x)

            st.session_state[df_hotel][coluna] = pd.to_numeric(st.session_state[df_hotel][coluna], errors='coerce')

        for coluna in ['Bus', 'Micro', 'Van', 'Utilitario']:

            st.session_state[df_hotel][coluna] = st.session_state[df_hotel][coluna].apply(lambda x: None if pd.isna(x) or str(x).strip() == '' else x)

        st.session_state[df_hotel]['Sequência'] = pd.to_numeric(st.session_state[df_hotel]['Sequência'], errors='coerce')

def gerar_itens_faltantes(df_servicos, df_hoteis):

    lista_hoteis_df_router = df_servicos['Est Origem'].unique().tolist()

    lista_hoteis_sequencia = df_hoteis['Est Origem'].unique().tolist()

    itens_faltantes = set(lista_hoteis_df_router) - set(lista_hoteis_sequencia)

    itens_faltantes = list(itens_faltantes)

    return itens_faltantes, lista_hoteis_df_router

def inserir_hoteis_faltantes(itens_faltantes, aba_excel, regiao, id_gsheet):

    df_itens_faltantes = pd.DataFrame(itens_faltantes, columns=['Est Origem'])

    st.dataframe(df_itens_faltantes, hide_index=True)

    df_itens_faltantes[['Região', 'Sequência', 'Bus', 'Micro', 'Van', 'Utilitario', 'Hoteis Juntos p/ Apoios', 'Hoteis Juntos p/ Carro Principal']]=''

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(aba_excel)
    sheet_data = sheet.get_all_values()
    last_filled_row = len(sheet_data)
    data = df_itens_faltantes.values.tolist()
    start_row = last_filled_row + 1
    start_cell = f"A{start_row}"
    
    sheet.update(start_cell, data)

    st.error(f'Os hoteis acima não estão cadastrados na lista de sequência de hoteis. Eles foram inseridos no final da lista de {regiao}. Por favor, coloque-os na sequência e tente novamente')

def criar_df_2(df, df_hoteis):

    # Criando coluna de paxs totais

    df['Total ADT | CHD'] = df['Total ADT'] + df['Total CHD']

    # Criando colunas Micro Região e Sequência através de pd.merge

    df = pd.merge(df, df_hoteis, on='Est Origem', how='left')

    # Ordenando dataframe 

    df = df.sort_values(by='Sequência', ascending=False).reset_index(drop=True)

    # Criando colunas Roteiro e Carros

    df['Carros']=0

    return df

def roteirizar_reserve_service_mais_pax_max(df):

    df_reserve_service_pax_max = df.groupby(['Id_Servico', 'Est Origem']).agg({'Total ADT | CHD': 'sum'}).reset_index()

    df_reserve_service_pax_max = df_reserve_service_pax_max[df_reserve_service_pax_max['Total ADT | CHD']>=st.session_state.df_carros_principais['Capacidade'].max()]

    if len(df_reserve_service_pax_max)>0:

        df_reserve_service_pax_max['Carros'] = range(1, len(df_reserve_service_pax_max)+1)

        df_reserve_service_pax_max = df_reserve_service_pax_max[['Id_Servico', 'Carros']]

        df = df[~df['Id_Servico'].isin(df_reserve_service_pax_max['Id_Servico'])].reset_index(drop=True)

    return df, df_reserve_service_pax_max

def encontrar_melhor_combinacao(pax_list, id_list, capacidade):

    melhor_combo = None

    melhor_total_pax = 0

    for r in range(1, len(pax_list) + 1):

        for combo in combinations(zip(pax_list, id_list), r):

            total_pax = sum(p for p, _ in combo)

            if total_pax <= capacidade and total_pax > melhor_total_pax:

                melhor_combo = combo

                melhor_total_pax = total_pax

                if melhor_total_pax == capacidade:
                    return melhor_combo, melhor_total_pax

    return melhor_combo, melhor_total_pax

def agrupar_em_carros(pax_list, id_list, capacidade):
    carros = []

    while sum(pax_list) >= capacidade:
        melhor_combo, _ = encontrar_melhor_combinacao(pax_list, id_list, capacidade)

        if melhor_combo:

            ids_combo = [id_ for _, id_ in melhor_combo]
            carros.append(ids_combo)

            # Remove os IDs e passageiros alocados
            for _, id_ in melhor_combo:
                index = id_list.index(id_)
                del pax_list[index]
                del id_list[index]
        else:
            break

    return carros

def roteirizar_hoteis_mais_pax_max(df):

    df_hoteis_pax_max = df.groupby(['Est Origem']).agg({'Total ADT | CHD': ['sum', lambda x: list(x)], 'Id_Servico': lambda x: list(x)}).reset_index()

    df_hoteis_pax_max.columns = ['Est Origem', 'Total ADT | CHD - Sum', 'Total ADT | CHD - List', 'Id_Servico']

    df_hoteis_pax_max = df_hoteis_pax_max[df_hoteis_pax_max['Total ADT | CHD - Sum']>=st.session_state.df_carros_principais['Capacidade'].max()]

    if len(df_hoteis_pax_max)>0:

        lista_ids_carros = []

        for index, row in df_hoteis_pax_max.iterrows():

            pax_list = row['Total ADT | CHD - List']
            
            id_list = row['Id_Servico']

            carros = agrupar_em_carros(pax_list, id_list, st.session_state.df_carros_principais['Capacidade'].max())

            lista_ids_carros.extend(carros)

        df_final = pd.DataFrame({'Id_Servico': lista_ids_carros})

        if len(df_final)>0:

            df_final['Carros'] = range(1, len(df_final)+1)

            df = df[~df['Id_Servico'].isin(df_final['Id_Servico'].explode())].reset_index(drop=True)

    else:

        df_final = df_hoteis_pax_max

    return df, df_final

def objetos_parametros(row):

    if st.session_state.servico_roteiro=='CITY TOUR':

        with row[0]:

            horario_passeio = st.time_input('Horário Padrão de Último Hotel', time(8,40), 'horario_passeio', step=300)

    elif st.session_state.servico_roteiro=='PRAIAS DA COSTA DO CONDE':

        with row[0]:

            horario_passeio = st.time_input('Horário Padrão de Último Hotel', time(7,30), 'horario_passeio', step=300)

    elif st.session_state.servico_roteiro=='ILHA DE AREIA VERMELHA':

        with row[0]:

            horario_passeio = st.time_input('Horário Padrão de Último Hotel', time(7,30), 'horario_passeio', step=300)

    elif st.session_state.servico_roteiro=='LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ':

        with row[0]:

            horario_passeio = st.time_input('Horário Padrão de Último Hotel', time(7,30), 'horario_passeio', step=300)

    elif st.session_state.servico_roteiro=='ENTARDECER NA PRAIA DO JACARÉ ':

        with row[0]:

            horario_passeio = st.time_input('Horário Padrão de Último Hotel', time(7,30), 'horario_passeio', step=300)
    
    with row[1]:

        max_hoteis = st.number_input('Máximo de Hoteis por Carro', step=1, value=10, key='max_hoteis')

def gerar_variaveis_dict_regioes_hoteis(servico_roteiro):

    nome_df_hotel = st.session_state.dict_regioes_hoteis[servico_roteiro][0]

    nome_html_ref = st.session_state.dict_regioes_hoteis[servico_roteiro][1]

    nome_aba_excel = st.session_state.dict_regioes_hoteis[servico_roteiro][2]

    nome_regiao = st.session_state.dict_regioes_hoteis[servico_roteiro][3]

    return nome_df_hotel, nome_html_ref, nome_aba_excel, nome_regiao

def contabilizar_paxs_hotel_juncao(row, df_group_hoteis_juntos):

    if row['Hoteis Juntos p/ Carro Principal']!='':

        paxs_hotel=df_group_hoteis_juntos[df_group_hoteis_juntos['Hoteis Juntos p/ Carro Principal']==row['Hoteis Juntos p/ Carro Principal']]['Total ADT | CHD']

    else:

        paxs_hotel=row['Total ADT | CHD']

    return paxs_hotel

def gerar_df_groups(df_servicos):

    df_servicos['Região'] = df_servicos['Região'].replace('MANAIRA 1', 'MANAIRA')

    df_servicos['Hoteis Juntos p/ Carro Principal'] = df_servicos['Hoteis Juntos p/ Carro Principal'].fillna('')

    df_group = df_servicos.groupby('Est Origem').agg({'Total ADT | CHD': 'sum', 'Hoteis Juntos p/ Carro Principal': 'first', 'Sequência': 'first', 'Id_Servico': lambda x: list(x), 
                                                      'Região': 'first'}).reset_index()

    df_group = df_group.sort_values(by='Sequência', ascending=False).reset_index(drop=True)

    df_group_hoteis_juntos = df_group.groupby('Hoteis Juntos p/ Carro Principal')['Total ADT | CHD'].sum().reset_index()

    df_group_hoteis_juntos = df_group_hoteis_juntos[df_group_hoteis_juntos['Hoteis Juntos p/ Carro Principal']!=''].reset_index(drop=True)

    df_group_regiao = df_servicos.groupby(['Região']).agg({'Total ADT | CHD': 'sum', 'Sequência': 'max', 'Id_Servico': lambda x: list(x), 'Est Origem': 'nunique'}).reset_index()

    df_group_regiao = df_group_regiao.sort_values(by='Sequência', ascending=False).reset_index(drop=True)

    return df_group, df_group_hoteis_juntos, df_group_regiao

def abrir_novo_carro(df_rota_gerada, index, paxs_hotel, carro_principal):

    df_rota_gerada.at[index, 'Carros'] = df_rota_gerada['Carros'].max()+1

    paxs_total_carro = paxs_hotel

    contador_hoteis = 1

    if carro_principal+1<len(st.session_state.df_carros_principais):

        carro_principal+=1

    else:

        carro_principal=0

    pax_max = int(st.session_state.df_carros_principais.at[carro_principal, 'Capacidade'])

    return df_rota_gerada, paxs_total_carro, contador_hoteis, pax_max, carro_principal

def gerar_divisao_carros(df_group, df_group_hoteis_juntos, max_hoteis):

    df_rota_gerada = df_group.copy()

    paxs_total_carro = 0

    contador_hoteis = 0

    carro_principal = 0

    pax_max = int(st.session_state.df_carros_principais.at[carro_principal, 'Capacidade'])

    for index, row in df_rota_gerada.iterrows():

        if index==0:

            contador_hoteis+=1

            paxs_hotel = contabilizar_paxs_hotel_juncao(row, df_group_hoteis_juntos)

            paxs_total_carro+=paxs_hotel

            df_rota_gerada.at[index, 'Carros'] = 1

        elif df_rota_gerada.at[index-1, 'Hoteis Juntos p/ Carro Principal']==row['Hoteis Juntos p/ Carro Principal'] and row['Hoteis Juntos p/ Carro Principal']!='':

            df_rota_gerada.at[index, 'Carros'] = df_rota_gerada.at[index-1, 'Carros']

        else:

            contador_hoteis+=1

            paxs_hotel = contabilizar_paxs_hotel_juncao(row, df_group_hoteis_juntos)

            if contador_hoteis>max_hoteis or paxs_total_carro+paxs_hotel>pax_max:

                df_rota_gerada, paxs_total_carro, contador_hoteis, pax_max, carro_principal = abrir_novo_carro(df_rota_gerada, index, paxs_hotel, carro_principal)

            else:

                paxs_total_carro+=paxs_hotel

                df_rota_gerada.at[index, 'Carros'] = df_rota_gerada.at[index-1, 'Carros']

    df_total_paxs_carros = df_rota_gerada.groupby('Carros')['Total ADT | CHD'].sum()

    df_rota_gerada = df_rota_gerada.explode('Id_Servico').groupby('Carros').agg({'Id_Servico': lambda x: list(x), 'Est Origem': lambda x: len(set(list(x)))}).reset_index()

    df_rota_gerada = pd.merge(df_rota_gerada, df_total_paxs_carros, on='Carros', how='left')

    n_carros = df_rota_gerada['Carros'].max()

    return n_carros, df_rota_gerada

def gerar_roteiros_alternativos_1(df_servicos, df_group, df_group_hoteis_juntos):

    total_hoteis = df_servicos['Est Origem'].sum()

    max_hoteis = math.ceil(total_hoteis / len(df_servicos))

    n_carros_alt_1, df_rota_alternativa_1 = gerar_divisao_carros(df_group, df_group_hoteis_juntos, max_hoteis)

    return n_carros_alt_1, df_rota_alternativa_1

def gerar_roteiros_alternativos_3(df_group, df_group_hoteis_juntos, df_group_regiao):

    df_rota_gerada = df_group.copy()

    paxs_total_carro = 0

    contador_hoteis = 0

    carro_principal = 0

    pax_max = int(st.session_state.df_carros_principais.at[carro_principal, 'Capacidade'])

    for index, row in df_rota_gerada.iterrows():

        if index==0:

            contador_hoteis+=1

            paxs_hotel = contabilizar_paxs_hotel_juncao(row, df_group_hoteis_juntos)

            paxs_total_carro+=paxs_hotel

            df_rota_gerada.at[index, 'Carros'] = 1

        elif df_rota_gerada.at[index-1, 'Hoteis Juntos p/ Carro Principal']==row['Hoteis Juntos p/ Carro Principal'] and row['Hoteis Juntos p/ Carro Principal']!='':

            df_rota_gerada.at[index, 'Carros'] = df_rota_gerada.at[index-1, 'Carros']

        elif df_rota_gerada.at[index-1, 'Região']!=row['Região']:

            n_hoteis_novo_bairro = df_group_regiao[df_group_regiao['Região']==row['Região']]['Est Origem'].iloc[0]

            paxs_novo_bairro = df_group_regiao[df_group_regiao['Região']==row['Região']]['Total ADT | CHD'].iloc[0]

            if n_hoteis_novo_bairro+contador_hoteis<=st.session_state.max_hoteis and paxs_total_carro+paxs_novo_bairro<=pax_max:

                contador_hoteis+=1

                paxs_hotel = contabilizar_paxs_hotel_juncao(row, df_group_hoteis_juntos)

                if contador_hoteis>st.session_state.max_hoteis or paxs_total_carro+paxs_hotel>pax_max:

                    df_rota_gerada, paxs_total_carro, contador_hoteis, pax_max, carro_principal = abrir_novo_carro(df_rota_gerada, index, paxs_hotel, carro_principal)

                else:

                    paxs_total_carro+=paxs_hotel

                    df_rota_gerada.at[index, 'Carros'] = df_rota_gerada.at[index-1, 'Carros']

            else:

                df_rota_gerada, paxs_total_carro, contador_hoteis, pax_max, carro_principal = abrir_novo_carro(df_rota_gerada, index, paxs_hotel, carro_principal)

        else:

            contador_hoteis+=1

            paxs_hotel = contabilizar_paxs_hotel_juncao(row, df_group_hoteis_juntos)

            if contador_hoteis>st.session_state.max_hoteis or paxs_total_carro+paxs_hotel>pax_max:

                df_rota_gerada.at[index, 'Carros'] = df_rota_gerada['Carros'].max()+1

                paxs_total_carro = paxs_hotel

                contador_hoteis = 1

            else:

                paxs_total_carro+=paxs_hotel

                df_rota_gerada.at[index, 'Carros'] = df_rota_gerada.at[index-1, 'Carros']

    df_total_paxs_carros = df_rota_gerada.groupby('Carros')['Total ADT | CHD'].sum()

    df_rota_gerada = df_rota_gerada.explode('Id_Servico').groupby('Carros').agg({'Id_Servico': lambda x: list(x), 'Est Origem': lambda x: len(set(list(x))), 'Região': 'unique'}).reset_index()

    df_rota_gerada = pd.merge(df_rota_gerada, df_total_paxs_carros, on='Carros', how='left')

    n_carros = df_rota_gerada['Carros'].max()

    return n_carros, df_rota_gerada

def agrupar_em_carros_rota_alt_4(pax_list, id_list):

    carro_principal = 0

    capacidade = int(st.session_state.df_carros_principais.at[carro_principal, 'Capacidade'])

    carros = []

    while sum(pax_list) > capacidade:

        melhor_combo, _ = encontrar_melhor_combinacao(pax_list, id_list, capacidade)

        if melhor_combo:

            ids_combo = [id_ for _, id_ in melhor_combo]
            carros.append(ids_combo)

            # Remove os IDs e passageiros alocados
            for _, id_ in melhor_combo:
                index = id_list.index(id_)
                del pax_list[index]
                del id_list[index]
        else:
            break

        carro_principal+=1

        capacidade = int(st.session_state.df_carros_principais.at[carro_principal, 'Capacidade'])

    ids_combo = [id_ for _, id_ in zip(pax_list, id_list)]

    carros.append(ids_combo)

    return carros

def gerar_roteiros_alternativos_4(df):

    df_servico = df.copy()

    df_servico['Servico'] = 'Passeio'

    df_servico = df_servico.groupby('Servico').agg({'Id_Servico': lambda x: list(x), 'Total ADT | CHD': lambda x: list(x)}).reset_index()

    pax_list = df_servico['Total ADT | CHD'].iloc[0]
    
    id_list = df_servico['Id_Servico'].iloc[0]

    carros = agrupar_em_carros_rota_alt_4(pax_list, id_list)

    lista_ids_carro = []

    for lista_listas_ids_hoteis in carros:

        lista_ref = []

        for lista_ids_hoteis in lista_listas_ids_hoteis:

            lista_ref.extend(lista_ids_hoteis)

        lista_ids_carro.append(lista_ref)

    df_final = pd.DataFrame({'Id_Servico': lista_ids_carro})

    df_final['Carros'] = range(1, len(df_final)+1)

    n_carros = df_final['Carros'].max()

    return n_carros, df_final

def gerar_roteiros_diferentes(df_rota_principal, df_rota_alternativa_1, df_rota_alternativa_2, df_rota_alternativa_3, df_rota_alternativa_4):

        df_rota_principal['Rota'] = 'Rota Principal'

        df_rota_alternativa_1['Rota'] = 'Rota Alternativa 1'

        df_rota_alternativa_2['Rota'] = 'Rota Alternativa 2'

        df_rota_alternativa_3['Rota'] = 'Rota Alternativa 3'

        df_rota_alternativa_4['Rota'] = 'Rota Alternativa 4'

        dfs = [df_rota_principal, df_rota_alternativa_1, df_rota_alternativa_2, df_rota_alternativa_3, df_rota_alternativa_4]

        dfs_unicos = []

        ja_registrados = set()

        coluna_ignorada = 'Rota'

        for i in range(len(dfs)):

            if i in ja_registrados:

                continue

            df_ref = dfs[i].drop(columns=[coluna_ignorada], errors='ignore')

            dfs_unicos.append(dfs[i])

            for j in range(i + 1, len(dfs)):  

                df_comp = dfs[j].drop(columns=[coluna_ignorada], errors='ignore')

                if df_ref.equals(df_comp):  

                    ja_registrados.add(j)

        df_final = pd.concat(dfs_unicos, ignore_index=True)

        return df_final

def gerar_lista_payload_escalar(data_roteiro):

    escalas_para_atualizar = []

    for index, row in st.session_state.df_escalar.iterrows():

        if row['Principal | Apoio']=='Principal':

            id_servicos = st.session_state.df_roteiros[(st.session_state.df_roteiros['Rota']==row['Rota']) & (st.session_state.df_roteiros['Carros']==row['Carros'])]['Id_Servico'].iloc[0]

        else:

            id_servicos = st.session_state.ids_apoio

        date_str = data_roteiro.strftime('%Y-%m-%d')

        id_veiculo = int(st.session_state.df_veiculos[st.session_state.df_veiculos['Veiculo']==row['Veiculo']]['id'].iloc[0])

        id_motorista = int(st.session_state.df_motoristas[st.session_state.df_motoristas['Motorista']==row['Motorista']]['id'].iloc[0])

        if row['Principal | Apoio']=='Principal':

            if row['Guia']!='':

                id_guia = int(st.session_state.df_guias[st.session_state.df_guias['Guia']==row['Guia']]['id'].iloc[0])

                payload = {
                        "date": date_str,
                        "vehicle_id": id_veiculo,
                        "driver_id": id_motorista,
                        "guide_id": id_guia,
                        "reserve_service_ids": id_servicos,
                    }
                
            else:

                payload = {
                        "date": date_str,
                        "vehicle_id": id_veiculo,
                        "driver_id": id_motorista,
                        "reserve_service_ids": id_servicos,
                    }
                
        else:

            if row['Guia']!='':

                id_guia = int(st.session_state.df_guias[st.session_state.df_guias['Guia']==row['Guia']]['id'].iloc[0])

                payload = {
                        "date": date_str,
                        "vehicle_id": id_veiculo,
                        "driver_id": id_motorista,
                        "guide_id": id_guia,
                        "roadmap_aux_id": None,
                        "reserve_service_ids": id_servicos,
                    }
                
            else:

                payload = {
                        "date": date_str,
                        "vehicle_id": id_veiculo,
                        "driver_id": id_motorista,
                        "roadmap_aux_id": None,
                        "reserve_service_ids": id_servicos,
                    }
        
        escalas_para_atualizar.append(payload)

    return escalas_para_atualizar

def update_scale(payload):

    if not 'roadmap_aux_id' in payload:

        try:
            response = requests.post(st.session_state.base_url_post, json=payload, verify=False)
            response.raise_for_status()
            return 'Escala atualizada com sucesso!'
        except requests.RequestException as e:
            st.error(f"Ocorreu um erro: {e}")
            return 'Erro ao atualizar a escala'
        
    else:

        try:
            response = requests.post(st.session_state.base_url_post_apoio, json=payload, verify=False)
            response.raise_for_status()
            return 'Escala atualizada com sucesso!'
        except requests.RequestException as e:
            st.error(f"Ocorreu um erro: {e}")
            return 'Erro ao atualizar a escala'

def colher_dados_escalas(row_rota):

    with row_rota[0]:

        rota_selecionada = st.selectbox('Selecionar Rota', st.session_state.df_roteiros['Rota'].unique(), index=None)

        inserir_escala = st.button('Inserir Escala')

    with row_rota[1]:

        principal_apoio = st.selectbox('Principal | Apoio', ['Principal', 'Apoio'], index=None)

    with row_rota[2]:

        veiculo = st.selectbox('Veículo', sorted(st.session_state.df_veiculos[~st.session_state.df_veiculos['Veiculo'].str.contains('4X4|4x4|BUGGY')]['Veiculo']), index=None)

        limpar_escalas = st.button('Limpar Escalas')

    with row_rota[3]:

        motorista = st.selectbox('Motorista', sorted(st.session_state.df_motoristas['Motorista']), index=None)

    with row_rota[4]:

        guia = st.selectbox('Guia', sorted(st.session_state.df_guias['Guia']), index=None)

    return rota_selecionada, inserir_escala, veiculo, limpar_escalas, motorista, guia, principal_apoio

def inserir_linha_df_escalar(inserir_escala, rota_selecionada, guia, veiculo, motorista, principal_apoio):

    if inserir_escala and (len(st.session_state.df_escalar[st.session_state.df_escalar['Principal | Apoio']=='Principal'])\
        <len(st.session_state.df_roteiros[st.session_state.df_roteiros['Rota']==rota_selecionada]) or principal_apoio=='Apoio'):

        if guia:

            st.session_state.df_escalar.loc[len(st.session_state.df_escalar)] = [rota_selecionada, principal_apoio, len(st.session_state.df_escalar)+1, veiculo, motorista, guia]

        else:

            st.session_state.df_escalar.loc[len(st.session_state.df_escalar)] = [rota_selecionada, principal_apoio, len(st.session_state.df_escalar)+1, veiculo, motorista, '']

    elif inserir_escala:

        st.error(f"Essa rota só pode ter {len(st.session_state.df_roteiros[st.session_state.df_roteiros['Rota']==rota_selecionada])} veículos")

def plotar_roteiros():

    for n_rota in st.session_state.df_roteiros['Rota'].unique():

        st.divider()

        row3 = st.columns(3)

        coluna=0

        df_group_rota = st.session_state.df_roteiros[st.session_state.df_roteiros['Rota']==n_rota]

        for index, row in df_group_rota.iterrows():

            df_carro = st.session_state.df_router_filtrado_2[st.session_state.df_router_filtrado_2['Id_Servico'].isin(row['Id_Servico'])].groupby(['Est Origem', 'Sequência'])\
                ['Total ADT | CHD'].sum().reset_index()

            df_carro = df_carro.sort_values(by='Sequência')

            with row3[coluna]:

                container = st.container(border=True, height=500)

                container.subheader(f"{n_rota} | Carro {int(row['Carros'])} | {int(df_carro['Total ADT | CHD'].sum())} Paxs | {len(df_carro)} Hoteis")

                container.dataframe(df_carro[['Est Origem', 'Total ADT | CHD']], hide_index=True, use_container_width=True)

                if coluna==2:

                    coluna=0

                else:

                    coluna+=1
                    
st.set_page_config(layout='wide')

st.title('Roteirizador de Passeios')

st.divider()

if not 'base_luck' in st.session_state:

    st.session_state.base_luck = 'test_phoenix_joao_pessoa'

if not 'df_veiculos_roteiro' in st.session_state:

    st.session_state.df_veiculos_roteiro = pd.DataFrame(columns=['Capacidade', 'Principal | Apoio'])

if not 'id_gsheet' in st.session_state:

    st.session_state.id_gsheet = '1vbGeqKyM4VSvHbMiyiqu1mkwEhneHi28e8cQ_lYMYhY'

    st.session_state.dict_abas_df_hoteis = {'Hoteis Sentido Sul': 'df_sentido_sul', 'Hoteis Joao Pessoa': 'df_sentido_norte'}

    st.session_state.dict_regioes_hoteis = {'CITY TOUR': ['df_sentido_sul', 'City Tour', 'Hoteis Sentido Sul', 'City Tour'], 
                                            'PRAIAS DA COSTA DO CONDE': ['df_sentido_sul', 'Conde', 'Hoteis Sentido Sul', 'Conde'], 
                                            'ILHA DE AREIA VERMELHA': ['df_sentido_norte', 'Ilha', 'Hoteis Joao Pessoa', 'Ilha'],
                                            'LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ': ['df_sentido_norte', 'Litoral Norte', 'Hoteis Joao Pessoa', 'Litoral Norte'],
                                            'ENTARDECER NA PRAIA DO JACARÉ ': ['df_sentido_norte', 'Entardecer', 'Hoteis Joao Pessoa', 'Entardecer']}

    st.session_state.base_url_post = 'https://driverjoao_pessoa.phoenix.comeialabs.com/scale/roadmap/allocate'

    st.session_state.base_url_post_apoio = 'https://driverjoao_pessoa.phoenix.comeialabs.com/scale/roadmap/aux'
    
if not 'df_router' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

if not 'df_escalar' in st.session_state:

    st.session_state.df_escalar = pd.DataFrame(columns=['Rota', 'Principal | Apoio', 'Carros', 'Veiculo', 'Motorista', 'Guia'])

row0=st.columns(3) 

row1=st.columns(3)

st.divider()

st.subheader('Parâmetros')

row2=st.columns(3)

with row0[0]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    if atualizar_phoenix:

        puxar_dados_phoenix()

with row1[0]:

    container_roteirizar = st.container(border=True)

    data_roteiro = container_roteirizar.date_input('Data', value=None, format='DD/MM/YYYY', key='data_roteiro')

    lista_servicos = st.session_state.df_router[(st.session_state.df_router['Data Execucao']==data_roteiro) & (st.session_state.df_router['Tipo de Servico']=='TOUR') & 
                                                (st.session_state.df_router['Modo do Servico']=='REGULAR') & (st.session_state.df_router['Status do Servico']!='CANCELADO') & 
                                                (~st.session_state.df_router['Servico'].str.contains('COMBO FLEX|EMBARCAÇÃO - '))]['Servico'].unique().tolist()

    servico_roteiro = container_roteirizar.selectbox('Serviço', sorted(lista_servicos), index=None, placeholder='Escolha um Serviço', 
                                                     key='servico_roteiro')  

    row_container = container_roteirizar.columns(2)

    with row_container[0]:

        roteirizar = st.button('Roteirizar')

with row1[1]:

    container_inserir_veiculos = st.container(border=True)

    capacidade_veiculo = container_inserir_veiculos.number_input('Capacidade Veículo', value=46)

    principal_apoio = container_inserir_veiculos.radio('Principal / Apoio', ['Principal', 'Apoio'])

    inserir_veiculos = container_inserir_veiculos.button('Inserir Veículos')

    if inserir_veiculos:

        lista_insercao = [capacidade_veiculo, principal_apoio]

        st.session_state.df_veiculos_roteiro.loc[len(st.session_state.df_veiculos_roteiro)] = lista_insercao

with row1[2]:

    limpar_veiculos = st.button('Limpar Veículos')

    if limpar_veiculos:

        st.session_state.df_veiculos_roteiro = pd.DataFrame(columns=['Capacidade', 'Principal | Apoio'])

    container_dataframe = st.container()

    container_dataframe.dataframe(st.session_state.df_veiculos_roteiro, hide_index=True, use_container_width=True)

if servico_roteiro:

    objetos_parametros(row2)

if roteirizar and data_roteiro and servico_roteiro and len(st.session_state.df_veiculos_roteiro)>0:

    with st.spinner('Puxando sequências de hoteis...'):

        puxar_sequencias_hoteis(st.session_state.id_gsheet, st.session_state.dict_abas_df_hoteis)

    nome_df_hotel, nome_html_ref, nome_aba_excel, nome_regiao = gerar_variaveis_dict_regioes_hoteis(servico_roteiro)

    df_hoteis_ref = st.session_state[nome_df_hotel]

    st.session_state.df_carros_principais = st.session_state.df_veiculos_roteiro[st.session_state.df_veiculos_roteiro['Principal | Apoio']=='Principal']\
        .sort_values(by='Capacidade', ascending=False).reset_index(drop=True)

    if servico_roteiro=='CITY TOUR':

        st.session_state.df_carros_principais.loc[st.session_state.df_carros_principais['Principal | Apoio']=='Principal', 'Capacidade'] = \
            st.session_state.df_carros_principais.loc[st.session_state.df_carros_principais['Principal | Apoio']=='Principal', 'Capacidade'].apply(lambda x: math.ceil(x*1.15))

    df_router_filtrado = st.session_state.df_router[(st.session_state.df_router['Data Execucao']==data_roteiro) & (st.session_state.df_router['Tipo de Servico']=='TOUR') & 
                                                    (st.session_state.df_router['Modo do Servico']=='REGULAR') & (st.session_state.df_router['Status do Servico']!='CANCELADO') & 
                                                    (st.session_state.df_router['Servico']==servico_roteiro)].reset_index(drop=True)
    
    df_router_filtrado = df_router_filtrado[~df_router_filtrado['Observacao'].str.upper().str.contains('CLD', na=False)]

    itens_faltantes, lista_hoteis_df_router = gerar_itens_faltantes(df_router_filtrado, df_hoteis_ref)

    if len(itens_faltantes)==0:

        df_router_filtrado_2 = criar_df_2(df_router_filtrado, df_hoteis_ref)

        st.session_state.df_router_filtrado_2 = df_router_filtrado_2

        df_router_filtrado_2, df_reserve_service_pax_max = roteirizar_reserve_service_mais_pax_max(df_router_filtrado_2)

        df_router_filtrado_2, df_hoteis_pax_max = roteirizar_hoteis_mais_pax_max(df_router_filtrado_2)

        df_group, df_group_hoteis_juntos, df_group_regiao = gerar_df_groups(df_router_filtrado_2)

        n_carros, df_rota_principal = gerar_divisao_carros(df_group, df_group_hoteis_juntos, st.session_state.max_hoteis)

    else:

        inserir_hoteis_faltantes(itens_faltantes, nome_aba_excel, nome_regiao, st.session_state.id_gsheet)

        st.stop()

    if n_carros>1:

        n_carros_alt_1, df_rota_alternativa_1 = gerar_roteiros_alternativos_1(df_rota_principal, df_group, df_group_hoteis_juntos)

        n_carros_alt_2, df_rota_alternativa_2 = gerar_divisao_carros(df_group, df_group_hoteis_juntos, 13)

        n_carros_alt_3, df_rota_alternativa_3 = gerar_roteiros_alternativos_3(df_group, df_group_hoteis_juntos, df_group_regiao)

        n_carros_alt_4, df_rota_alternativa_4 = gerar_roteiros_alternativos_4(df_group)

        st.session_state.df_roteiros = gerar_roteiros_diferentes(df_rota_principal, df_rota_alternativa_1, df_rota_alternativa_2, df_rota_alternativa_3, 
                                                                 df_rota_alternativa_4)
        
    else:

        df_rota_principal['Rota'] = 'Rota Principal'

        st.session_state.df_roteiros = df_rota_principal

if 'df_roteiros' in st.session_state:

    st.divider()

    row_rota = st.columns(5)

    rota_selecionada, inserir_escala, veiculo, limpar_escalas, motorista, guia, principal_apoio = colher_dados_escalas(row_rota)

    inserir_linha_df_escalar(inserir_escala, rota_selecionada, guia, veiculo, motorista, principal_apoio)

    if limpar_escalas:

        st.session_state.df_escalar = pd.DataFrame(columns=['Rota', 'Principal | Apoio', 'Carros', 'Veiculo', 'Motorista', 'Guia'])

    container_dataframe = st.container()

    container_dataframe.dataframe(st.session_state.df_escalar, hide_index=True, use_container_width=True)

    escalar_roteiros = st.button('Escalar Rotas')

    if escalar_roteiros:

        payload_escalas = gerar_lista_payload_escalar(data_roteiro)

        with st.spinner('Criando escalas no Phoenix...'):

            for escala in payload_escalas:

                status = update_scale(escala)

    elif not rota_selecionada:

        plotar_roteiros()

    else:

        df_roteiro_selecionado = st.session_state.df_roteiros[st.session_state.df_roteiros['Rota']==rota_selecionada]

        escolher_carro = st.selectbox('Veículo c/ Apoio', df_roteiro_selecionado['Carros'].astype(int).unique())

        if escolher_carro:

            df_carro_selecionado = df_roteiro_selecionado[df_roteiro_selecionado['Carros']==escolher_carro]

            df_carro_selecionado = st.session_state.df_router_filtrado_2[st.session_state.df_router_filtrado_2['Id_Servico'].isin(df_carro_selecionado['Id_Servico'].iloc[0])]\
                .groupby(['Est Origem', 'Sequência']).agg({'Total ADT | CHD': 'sum', 'Id_Servico': lambda x: list(x)}).reset_index()

            df_exibicao = df_carro_selecionado[['Est Origem', 'Sequência', 'Total ADT | CHD']].sort_values(by='Sequência')

            df_exibicao = df_exibicao[['Est Origem', 'Total ADT | CHD']]

            gb = GridOptionsBuilder.from_dataframe(df_exibicao)
            gb.configure_selection('multiple', use_checkbox=True, header_checkbox=True)
            gb.configure_grid_options(domLayout='autoHeight')
            gb.configure_grid_options(domLayout='autoWidth')
            gridOptions = gb.build()

            grid_response = AgGrid(df_exibicao, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True)

            selected_rows = grid_response['selected_rows']

            if selected_rows is not None:

                st.write(f"Total de Paxs no Apoio = {selected_rows['Total ADT | CHD'].sum()}")

            inserir_apoios = st.button('Inserir Apoio')

            if inserir_apoios and selected_rows is not None:

                st.session_state.ids_apoio = df_carro_selecionado[df_carro_selecionado['Est Origem'].isin(selected_rows['Est Origem'])]['Id_Servico'].explode().tolist()

                st.success('Apoio inserido com sucesso!')
