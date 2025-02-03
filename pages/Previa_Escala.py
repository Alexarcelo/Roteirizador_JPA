import streamlit as st
import mysql.connector
import decimal
import pandas as pd
import gspread 
from datetime import datetime, timedelta, time
from st_aggrid import AgGrid, GridOptionsBuilder
from google.oauth2.service_account import Credentials
import requests
from google.cloud import secretmanager 
from google.oauth2 import service_account

def gerar_df_phoenix(vw_name, base_luck):

    data_hoje = datetime.now()
    data_hoje_str = data_hoje.strftime("%Y-%m-%d")
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
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

    st.session_state.df_router = gerar_df_phoenix('vw_router', 'test_phoenix_joao_pessoa')

    st.session_state.vw_atual = 'vw_previa'

    st.session_state.df_router = st.session_state.df_router[~(st.session_state.df_router['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO'])) & 
                                                            ~(st.session_state.df_router['Status do Servico'].isin(['CANCELADO'])) &
                                                            ~(pd.isna(st.session_state.df_router['Status da Reserva'])) & 
                                                            ~(st.session_state.df_router['Servico'].isin(['FAZER CONTATO - SEM TRF IN ']))].reset_index(drop=True)
    
    st.session_state.df_router['Data Horario Apresentacao Original'] = st.session_state.df_router['Data Horario Apresentacao']

    st.session_state.df_motoristas = gerar_df_phoenix('vw_motoristas', 'test_phoenix_joao_pessoa')

    st.session_state.df_motoristas = st.session_state.df_motoristas.rename(columns={'nickname': 'Motorista', 'id': 'id_motorista'})

    st.session_state.df_guias = gerar_df_phoenix('vw_guias', 'test_phoenix_joao_pessoa')

    st.session_state.df_guias = st.session_state.df_guias.rename(columns={'nickname': 'Guia', 'id': 'id_guia'})

    st.session_state.df_veiculos = gerar_df_phoenix('vw_veiculos', 'test_phoenix_joao_pessoa')

    st.session_state.df_veiculos = st.session_state.df_veiculos.rename(columns={'name': 'Veículo', 'id': 'id_veiculo'})

def criar_df_router_filtrado():

    # Puxando dataframe com Tours e Transfers da data selecionada

    df_router_filtrado = st.session_state.df_router[((st.session_state.df_router['Data Execucao']==data_roteiro) | 
                                                    (st.session_state.df_router['Data Execucao']==data_roteiro + timedelta(days=1))) & 
                                                    ~(st.session_state.df_router['Servico'].isin(['EXTRA', 'GUIA BASE NOTURNO'])) & 
                                                    ~(st.session_state.df_router['Servico'].str.upper().str.contains('COMBO FLEX'))].reset_index(drop=True)

    # Tirando CLDs

    df_router_filtrado = df_router_filtrado[~df_router_filtrado['Observacao'].str.upper().str.contains('CLD', na=False)].reset_index(drop=True)

    # Verificando Cadeirantes

    df_router_filtrado['Cadeirante'] = df_router_filtrado.apply(lambda row: 'X' if 'CADEIRANTE' in row['Observacao'] else '', axis=1)
        
    # Criando coluna com Total ADT | CHD

    df_router_filtrado['Total ADT | CHD'] = df_router_filtrado['Total ADT'] + df_router_filtrado['Total CHD']  

    # Ordenando por Servico e Horario Apresentacao

    df_router_filtrado['Data Horario Apresentacao'] = pd.to_datetime(df_router_filtrado['Data Horario Apresentacao'])

    df_router_filtrado['Horario Apresentacao'] = pd.to_datetime(df_router_filtrado['Data Horario Apresentacao'], format='%H:%M').dt.time

    df_router_filtrado = df_router_filtrado.sort_values(by=['Servico', 'Horario Apresentacao']).reset_index(drop=True)

    df_router_filtrado['Nome Original Servico'] = df_router_filtrado['Servico']

    return df_router_filtrado

def puxar_historico_roteiros_apoios(id_gsheet, nome_df, aba, nome_df_2, aba_2, nome_df_3, aba_3, nome_df_4, aba_4, nome_df_5, aba_5):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    st.session_state[nome_df]['Junção'] = pd.to_numeric(st.session_state[nome_df]['Junção'].replace('nan', 0))

    st.session_state[nome_df]['Carros'] = pd.to_numeric(st.session_state[nome_df]['Carros'])

    st.session_state[nome_df]['Roteiro'] = pd.to_numeric(st.session_state[nome_df]['Roteiro'])

    st.session_state[nome_df]['Id_Servico'] = pd.to_numeric(st.session_state[nome_df]['Id_Servico'])

    if 'Carros Apoios' in st.session_state[nome_df].columns.tolist():

        st.session_state[nome_df]['Carros Apoios'] = pd.to_numeric(st.session_state[nome_df]['Carros Apoios'])

    st.session_state[nome_df]['Total ADT | CHD'] = pd.to_numeric(st.session_state[nome_df]['Total ADT | CHD'])

    st.session_state[nome_df]['Data Execucao'] = pd.to_datetime(st.session_state[nome_df]['Data Execucao']).dt.date

    st.session_state[nome_df] = st.session_state[nome_df][(st.session_state[nome_df]['Data Execucao']==st.session_state.data_roteiro) | 
                                                          (st.session_state[nome_df]['Data Execucao']==st.session_state.data_roteiro + timedelta(days=1))].reset_index(drop=True)
    
    sheet = spreadsheet.worksheet(aba_2)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_2] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    sheet = spreadsheet.worksheet(aba_3)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_3] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    st.session_state[nome_df_3]['Data Execucao'] = pd.to_datetime(st.session_state[nome_df_3]['Data Execucao']).dt.date

    st.session_state[nome_df_3]['Embarque'] = pd.to_datetime(st.session_state[nome_df_3]['Embarque']).dt.time

    sheet = spreadsheet.worksheet(aba_4)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_4] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    sheet = spreadsheet.worksheet(aba_5)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_5] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def verificar_servicos_in_out_sem_roteiros(df_router_filtrado):

    lista_id_servico_in_out = df_router_filtrado[(df_router_filtrado['Tipo de Servico'].isin(['IN', 'OUT'])) & 
                                                 (~df_router_filtrado['Servico'].isin(['FAZER CONTATO - SEM TRF IN '])) & 
                                                 (~df_router_filtrado['Voo'].isin(['G3 - 0001'])) & 
                                                 (~df_router_filtrado['Observacao'].str.upper().str.contains('CLD', na=False))]['Id_Servico'].unique().tolist()

    lista_id_servico_in_out_roteiros = st.session_state.df_historico_roteiros['Id_Servico'].unique().tolist()

    id_servico_nao_roteirizados = list(set(lista_id_servico_in_out) - set(lista_id_servico_in_out_roteiros))

    if len(id_servico_nao_roteirizados)>0:

        st.error(f'Os serviços abaixo não foram roteirizados')

        container_dataframe = st.container()

        container_dataframe.dataframe(df_router_filtrado[df_router_filtrado['Id_Servico'].isin(id_servico_nao_roteirizados)]
                                      [['Data Execucao', 'Reserva', 'Modo do Servico', 'Tipo de Servico', 'Servico', 'Voo', 'Horario Voo']], hide_index=True, use_container_width=True)

def ajustar_nomenclatura_passeios_tt_pvt(row):

    info = f"PRIVATIVO | {row['Passeios | OUT']}\nObs.: {row['Observacao']}"

    if row['Cadeirante'] != '':

        info += "\nAtenção Paxs Cadeirante"
    
    if row['Est Origem'] == 'BA´RA HOTEL':

        info += "\nAtenção Paxs VIPs BARA"

    if pd.notna(row['Região Hotel']):

        info += f"\nAtenção HOTEL {row['Região Hotel']}"

    if 'ESCADINHA' in row['Observacao']:

        info += "\nAtenção Colocar Escadinha"

    info += f"\nReserva: {row['Reserva']}"

    return info

def criar_df_tt_pvt_final(df_router_filtrado):

    # Filtrando TOUR e TRANSFER privativo

    mask_tt_pvt = (df_router_filtrado['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & (df_router_filtrado['Modo do Servico']!='REGULAR') & \
        (df_router_filtrado['Data Execucao']==st.session_state.data_roteiro)

    df_tt_pvt = df_router_filtrado[mask_tt_pvt][['Reserva', 'Servico', 'Nome Original Servico', 'Total ADT | CHD', 'Horario Apresentacao', 'Cadeirante', 'Observacao', 'Est Origem', 'Id_Servico']]\
        .reset_index(drop=True)

    # Inserindo coluna de Região Hotel

    df_tt_pvt = pd.merge(df_tt_pvt, st.session_state.df_hoteis_pitimbu_camboinha, left_on='Est Origem', right_on='Hoteis', how='left')

    df_tt_pvt = df_tt_pvt.rename(columns={'Servico': 'Passeios | OUT', 'Total ADT | CHD': 'Paxs Passeios | OUT'})

    # Colocando Observação, pax cadeirante, vip Bara, hoteis camboinha ou pitimbu e reserva no nome do passeio

    df_tt_pvt['Passeios | OUT'] = df_tt_pvt.apply(ajustar_nomenclatura_passeios_tt_pvt, axis=1)

    # Selecionando colunas que vão ficar no dataframe final

    df_tt_pvt_final = df_tt_pvt[['Horario Apresentacao', 'Passeios | OUT', 'Paxs Passeios | OUT', 'Nome Original Servico', 'Id_Servico']].sort_values(by='Horario Apresentacao').reset_index(drop=True)

    return df_tt_pvt_final

def existe_x_na_coluna(series):

    return 'X' if 'X' in series.values else ''

def transformar_em_string(series):

    return ', '.join(list(set(series.dropna())))

def ajustar_nomenclatura_cadeirante_regiao_hotel(row):

    info = row['Passeios | OUT']

    if row['Cadeirante']!='':

        info += "\nAtenção Paxs Cadeirante"

    if row['Região Hotel']!='':

        info += f"\nAtenção HOTEL {row['Região Hotel']}"

    return info

def criar_df_tt_reg_final(df_router_filtrado):

    # Filtrando TOUR e TRANSFER regular

    mask_tt_reg = (df_router_filtrado['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & (df_router_filtrado['Modo do Servico']=='REGULAR') & \
        (df_router_filtrado['Data Execucao']==st.session_state.data_roteiro)

    df_tt_reg = df_router_filtrado[mask_tt_reg].reset_index(drop=True)

    # Inserindo coluna de Região Hotel

    df_tt_reg = pd.merge(df_tt_reg, st.session_state.df_hoteis_pitimbu_camboinha, left_on='Est Origem', right_on='Hoteis', how='left')

    # Agrupando serviços

    df_tt_reg_group = df_tt_reg.groupby(['Servico', 'Nome Original Servico']).agg({'Total ADT | CHD': 'sum', 'Horario Apresentacao': 'first', 'Cadeirante': existe_x_na_coluna, 
                                                                                   'Região Hotel': transformar_em_string}).reset_index()

    df_tt_reg_group = df_tt_reg_group.rename(columns={'Servico': 'Passeios | OUT', 'Total ADT | CHD': 'Paxs Passeios | OUT'})

    # Colocando aviso se existe pax cadeirante e hotel camboinha ou pitimbu

    df_tt_reg_group['Passeios | OUT'] = df_tt_reg_group.apply(ajustar_nomenclatura_cadeirante_regiao_hotel, axis=1)

    # Selecionando colunas que vão ficar no dataframe final

    df_tt_reg_group_final = df_tt_reg_group[['Horario Apresentacao', 'Passeios | OUT', 'Paxs Passeios | OUT', 'Nome Original Servico']].sort_values(by='Horario Apresentacao').reset_index(drop=True)
    
    return df_tt_reg_group_final

def criar_df_tt_final(df_tt_pvt_final, df_tt_reg_group_final): 

    df_tt_final = pd.concat([df_tt_pvt_final, df_tt_reg_group_final], ignore_index=True)

    df_tt_final = df_tt_final.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    return df_tt_final

def colocar_embarcacoes_no_topo(df_tt):

    df_tt.loc[df_tt['Passeios | OUT'].str[:13]=='EMBARCAÇÃO - ', 'Horario Apresentacao'] = time(0)

    df_tt = df_tt.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    return df_tt

def verificar_embarques(df_tt):

    existe_horario = df_tt['Embarque'].notna().any()

    existe_embarque_ilha = (df_tt['Passeios | OUT'].str[:22]=='ILHA DE AREIA VERMELHA').any()

    existe_embarque_extremo = (df_tt['Passeios | OUT'].str[:28]=='PISCINAS DO EXTREMO ORIENTAL').any()

    if (existe_embarque_ilha or existe_embarque_extremo) and (not existe_horario):

        st.error('Na data selecionada existe passeio de Ilha e/ou Extremo, mas não existe agenda de embarque cadastrada. Por favor, cadastre e tente novamente')

        st.stop()

def juntar_paxs_litoral_norte(df_tt):

    if (df_tt['Passeios | OUT']=='LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ').any() and (df_tt['Passeios | OUT']=='LITORAL NORTE/LUCENA COM ENTARDECER NA PRAIA DO JACARÉ').any():

        paxs_ln =  df_tt[df_tt['Passeios | OUT'].isin(['LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ', 'LITORAL NORTE/LUCENA COM ENTARDECER NA PRAIA DO JACARÉ'])]['Paxs Passeios | OUT'].sum()

        df_tt.loc[df_tt['Passeios | OUT']=='LITORAL NORTE COM ENTARDECER NA PRAIA DO JACARÉ', 'Paxs Passeios | OUT'] = paxs_ln

        df_tt = df_tt[df_tt['Passeios | OUT']!='LITORAL NORTE/LUCENA COM ENTARDECER NA PRAIA DO JACARÉ'].reset_index(drop=True)

    return df_tt

def inserir_deixar_quadrilha(df_tt):

    if (df_tt['Passeios | OUT']=='CATAMARÃ DO FORRÓ').any():

        deixar_trilha = [time(23), 'DEIXAR QUADRILHA', 13, 'DEIXAR QUADRILHA', None, None, None]

        df_tt.loc[len(df_tt)] = deixar_trilha

        df_tt = df_tt.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    return df_tt

def transformar_pax_em_lista(paxs, limite_paxs):

    paxs=int(paxs)
    
    lista_n_paxs = [limite_paxs] * (paxs // limite_paxs)

    restante = paxs % limite_paxs

    if restante:

        lista_n_paxs.append(restante)
    
    return lista_n_paxs

def identificando_quantidade_carros_trilhas(df_tt, limite_paxs):

    df_trilhas = df_tt[(df_tt['Passeios | OUT'].isin(['TRILHA DOS COQUEIRAIS', 'TRILHA DOS MIRANTES DA COSTA DO CONDE'])) & (df_tt['Paxs Passeios | OUT']>limite_paxs)].reset_index()

    df_trilhas['Paxs Passeios | OUT'] = df_trilhas['Paxs Passeios | OUT'].apply(transformar_pax_em_lista, args=(limite_paxs,))

    df_trilhas = df_trilhas.explode('Paxs Passeios | OUT')

    df_tt = df_tt.drop(index=df_trilhas['index'].unique())

    df_trilhas = df_trilhas.drop(columns='index')

    df_tt = pd.concat([df_tt, df_trilhas], ignore_index=True)

    df_tt = df_tt.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    return df_tt

def adicionar_embarque_ponto_de_apoio(row):

    info = row['Passeios | OUT']

    if pd.notna(row['Embarque']):

        info += f"\nEmbarque: {row['Embarque']}" 

    if pd.notna(row['Ponto de Apoio']):

        info += f"\nPonto de Apoio: {row['Ponto de Apoio']}"

    return info

def criar_df_out(df_router_filtrado):

    df_router_filtrado['Horario Voo'] = pd.to_datetime(df_router_filtrado['Horario Voo']).dt.time

    df_out_d1 = df_router_filtrado[(df_router_filtrado['Tipo de Servico']=='OUT') & (df_router_filtrado['Data Execucao']==data_roteiro) & 
                                   (df_router_filtrado['Horario Apresentacao']>time(4,0)) & (df_router_filtrado['Horario Voo']>time(4,0))].reset_index(drop=True)

    df_out_d1 = df_out_d1.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_out_d2 = df_router_filtrado[(df_router_filtrado['Tipo de Servico']=='OUT') & (df_router_filtrado['Data Execucao']==data_roteiro+timedelta(days=1)) & 
                                   ((df_router_filtrado['Horario Voo']<=time(4,0)) | (df_router_filtrado['Horario Apresentacao']<=time(4,0)))].reset_index(drop=True)

    df_out_d2 = df_out_d2.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_out = pd.concat([df_out_d1, df_out_d2], ignore_index=True)

    df_out['Total ADT | CHD'] = df_out['Total ADT'] + df_out['Total CHD']

    df_out['Região OUT'] = df_out['Servico'].apply(lambda x: 'JPA' if 'AEROPORTO JOÃO PESSOA' in x else 'REC' if 'AEROPORTO RECIFE' in x else 'CPV' if 'AEROPORTO CAMPINA GRANDE' in x 
                                                   else 'NAT' if 'AEROPORTO NATAL' in x else 'REGIÃO DESCONHECIDA')

    return df_out

def avisar_reg_g3_0001(df):

    tipo_servico = df['Tipo de Servico'].iloc[0]

    lista_voo_g3_0001_regular = df[(df['Voo'].isin(['G3 - 0001'])) & (df['Modo do Servico']=='REGULAR')]['Id_Servico'].unique().tolist()

    nomes_reservas = ', '.join(df[df['Id_Servico'].isin(lista_voo_g3_0001_regular)]['Reserva'].unique().tolist())

    if len(lista_voo_g3_0001_regular)>0:

        st.error(f'As reservas {nomes_reservas} estão com voo {tipo_servico} G3 - 0001 e são regulares.')

def ajustar_nomes_operadoras(df_out):

    dict_operadoras = dict(zip(st.session_state.df_operadoras.iloc[:, 0], st.session_state.df_operadoras.iloc[:, 1]))

    df_out['Parceiro'] = df_out['Parceiro'].replace(dict_operadoras)

    return df_out

def agrupar_roteiros_carros_nome_escala_out(df_out):

    df_out['Voo | Horario Voo'] = df_out['Voo'] + ' ' + df_out['Horario Voo'].astype(str)

    # Inserindo coluna de Região Hotel

    df_out = pd.merge(df_out, st.session_state.df_hoteis_pitimbu_camboinha, left_on='Est Origem', right_on='Hoteis', how='left')

    df_out['Id_Servico'] = df_out['Id_Servico'].astype(str)

    df_out_reg_group = df_out[df_out['Modo do Servico']=='REGULAR'].groupby(['Data Execucao', 'Modo do Servico', 'Roteiro', 'Carros', 'Servico', 'Região OUT'])\
        .agg({'Voo | Horario Voo': transformar_em_string, 'Total ADT | CHD': 'sum', 'Horario Apresentacao': 'min', 'Cadeirante': existe_x_na_coluna, 'Região Hotel': transformar_em_string, 
              'Id_Servico': transformar_em_string}).reset_index()
    
    df_voo_horario = df_out.groupby(['Roteiro', 'Carros', 'Voo | Horario Voo', 'Parceiro'])[['Total ADT | CHD']].sum().reset_index()  

    df_voo_horario['Parceiro | Paxs'] = df_voo_horario['Total ADT | CHD'].astype(int).astype(str) + ' ' + df_voo_horario['Parceiro']

    for index, row in df_out_reg_group.iterrows():

        lista_voos_horarios = row['Voo | Horario Voo'].split(', ')

        lista_nomes_escala = []

        for voo_horario in lista_voos_horarios:

            df_ref = df_voo_horario[(df_voo_horario['Roteiro']==row['Roteiro']) & (df_voo_horario['Carros']==row['Carros']) & (df_voo_horario['Voo | Horario Voo']==voo_horario)]

            nome_escala = f"TRF OUT {row['Região OUT']} | {voo_horario[:15]} | {' '.join(df_ref['Parceiro | Paxs'].unique())}"

            lista_nomes_escala.append(nome_escala)

        df_out_reg_group.loc[index, 'Passeios | OUT'] = '\n'.join(lista_nomes_escala)

    # Colocando aviso se existe pax cadeirante e hotel camboinha ou pitimbu

    df_out_reg_group['Passeios | OUT'] = df_out_reg_group.apply(ajustar_nomenclatura_cadeirante_regiao_hotel, axis=1)

    return df_out_reg_group[['Horario Apresentacao', 'Passeios | OUT', 'Total ADT | CHD', 'Roteiro', 'Carros', 'Id_Servico']]

def ajustar_nomenclatura_cadeirante_bara_regiao_hotel(row):

    info = row['Passeios | OUT']

    if row['Cadeirante']!='':

        info += "\nAtenção Paxs Cadeirante"

    if row['Est Origem'] == 'BA´RA HOTEL':

        info += "\nAtenção Paxs VIPs BARA"

    if row['Região Hotel']!='':

        info += f"\nAtenção HOTEL {row['Região Hotel']}"

    return info

def gerar_out_pvt(df_out):

    df_out_pvt_group = df_out[df_out['Modo do Servico']!='REGULAR']

    df_out_pvt_group['Passeios | OUT'] = 'PRIVATIVO | TRF OUT ' +  df_out_pvt_group['Região OUT'] + ' | ' + df_out_pvt_group['Voo | Horario Voo'].str[:15] + ' | ' + \
        df_out_pvt_group['Total ADT | CHD'].astype(int).astype(str) + ' PAXS\nReserva: ' + df_out_pvt_group['Reserva']
    
    # Inserindo coluna de Região Hotel
    
    df_out_pvt_group = pd.merge(df_out_pvt_group, st.session_state.df_hoteis_pitimbu_camboinha, left_on='Est Origem', right_on='Hoteis', how='left')
    
    # Colocando aviso se existe pax cadeirante

    df_out_pvt_group['Passeios | OUT'] = df_out_pvt_group.apply(ajustar_nomenclatura_cadeirante_bara_regiao_hotel, axis=1)
    
    return df_out_pvt_group[['Horario Apresentacao', 'Passeios | OUT', 'Total ADT | CHD', 'Roteiro', 'Carros', 'Id_Servico']]

def juntar_reg_pvt_out(df_out_reg_group, df_out_pvt_group):

    df_out_final = pd.concat([df_out_reg_group, df_out_pvt_group], ignore_index=True)

    df_out_final = df_out_final.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_out_final = df_out_final.rename(columns={'Total ADT | CHD': 'Paxs Passeios | OUT'})

    return df_out_final

def concat_tt_out_ordem_cronologica(df_tt, df_out_final):

    df_out_dia = df_out_final[df_out_final['Horario Apresentacao']>time(4)].sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_out_madrugada = df_out_final[df_out_final['Horario Apresentacao']<=time(4)].sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_tt_out = pd.concat([df_tt, df_out_dia], ignore_index=True)

    df_tt_out = df_tt_out.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_tt_out = pd.concat([df_tt_out, df_out_madrugada], ignore_index=True)

    return df_tt_out

def criar_df_in(df_router_filtrado):

    df_in = df_router_filtrado[(df_router_filtrado['Tipo de Servico']=='IN') & ((df_router_filtrado['Data Execucao']==data_roteiro) | 
                                                                                (df_router_filtrado['Data Execucao']==data_roteiro+timedelta(days=1)))].reset_index(drop=True)
    
    df_in['Região IN'] = df_in['Servico'].apply(lambda x: 'JPA' if 'AEROPORTO JOÃO PESSOA' in x else 'REC' if 'AEROPORTO RECIFE' in x else 'CPV' if 'AEROPORTO CAMPINA GRANDE' in x 
                                                else 'NAT' if 'AEROPORTO NATAL' in x else 'REGIÃO DESCONHECIDA')
    
    df_in_d1_jpa = df_in[(df_in['Região IN']=='JPA') & (df_in['Data Execucao']==data_roteiro) & (df_in['Horario Voo']>time(5,30))].reset_index(drop=True)

    df_in_d1_rec = df_in[(df_in['Região IN']=='REC') & (df_in['Data Execucao']==data_roteiro) & (df_in['Horario Voo']>time(7,30))].reset_index(drop=True)

    df_in_d1_cpv = df_in[(df_in['Região IN']=='CPV') & (df_in['Data Execucao']==data_roteiro) & (df_in['Horario Voo']>time(6,0))].reset_index(drop=True)

    df_in_d1_nat = df_in[(df_in['Região IN']=='NAT') & (df_in['Data Execucao']==data_roteiro) & (df_in['Horario Voo']>time(8,0))].reset_index(drop=True)
    
    df_in_d1 = pd.concat([df_in_d1_jpa, df_in_d1_rec, df_in_d1_cpv, df_in_d1_nat], ignore_index=True)

    df_in_d1 = df_in_d1.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_in_d2_jpa = df_in[(df_in['Região IN']=='JPA') & (df_in['Data Execucao']==data_roteiro+timedelta(days=1)) & (df_in['Horario Voo']<=time(5,30))].reset_index(drop=True)

    df_in_d2_rec = df_in[(df_in['Região IN']=='REC') & (df_in['Data Execucao']==data_roteiro+timedelta(days=1)) & (df_in['Horario Voo']<=time(7,30))].reset_index(drop=True)

    df_in_d2_cpv = df_in[(df_in['Região IN']=='CPV') & (df_in['Data Execucao']==data_roteiro+timedelta(days=1)) & (df_in['Horario Voo']<=time(6,0))].reset_index(drop=True)

    df_in_d2_nat = df_in[(df_in['Região IN']=='NAT') & (df_in['Data Execucao']==data_roteiro+timedelta(days=1)) & (df_in['Horario Voo']<=time(8,0))].reset_index(drop=True)

    df_in_d2 = pd.concat([df_in_d2_jpa, df_in_d2_rec, df_in_d2_cpv, df_in_d2_nat], ignore_index=True)

    df_in_d2 = df_in_d2.sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_in = pd.concat([df_in_d1, df_in_d2], ignore_index=True)

    df_in['Total ADT | CHD'] = df_in['Total ADT'] + df_in['Total CHD']

    return df_in

def ajustar_nomenclatura_cadeirante_regiao_hotel_in(row):

    info = row['IN']

    if row['Cadeirante']!='':

        info += "\nAtenção Paxs Cadeirante"

    if row['Região Hotel']!='':

        info += f"\nAtenção HOTEL {row['Região Hotel']}"

    return info

def agrupar_roteiros_carros_nome_escala_in(df_in):

    df_in['Voo | Horario Voo'] = df_in['Voo'] + ' ' + df_in['Horario Voo'].astype(str)

    # Inserindo coluna de Região Hotel

    df_in = pd.merge(df_in, st.session_state.df_hoteis_pitimbu_camboinha, left_on='Est Origem', right_on='Hoteis', how='left')

    df_in['Id_Servico'] = df_in['Id_Servico'].astype(str)

    df_in_reg_group = df_in[df_in['Modo do Servico']=='REGULAR'].groupby(['Data Execucao', 'Modo do Servico', 'Roteiro', 'Carros', 'Servico', 'Região IN'])\
        .agg({'Voo | Horario Voo': transformar_em_string, 'Total ADT | CHD': 'sum', 'Horario Apresentacao': 'min', 'Cadeirante': existe_x_na_coluna, 'Região Hotel': transformar_em_string, 
              'Id_Servico': transformar_em_string}).reset_index()
    
    df_voo_horario = df_in.groupby(['Roteiro', 'Carros', 'Voo | Horario Voo', 'Parceiro'])[['Total ADT | CHD']].sum().reset_index()  

    df_voo_horario['Parceiro | Paxs'] = df_voo_horario['Total ADT | CHD'].astype(int).astype(str) + ' ' + df_voo_horario['Parceiro']

    for index, row in df_in_reg_group.iterrows():

        lista_voos_horarios = row['Voo | Horario Voo'].split(', ')

        lista_nomes_escala = []

        for voo_horario in lista_voos_horarios:

            df_ref = df_voo_horario[(df_voo_horario['Roteiro']==row['Roteiro']) & (df_voo_horario['Carros']==row['Carros']) & (df_voo_horario['Voo | Horario Voo']==voo_horario)]

            nome_escala = f"TRF IN {row['Região IN']} | {voo_horario[:15]} | {' '.join(df_ref['Parceiro | Paxs'].unique())}"

            lista_nomes_escala.append(nome_escala)

        df_in_reg_group.loc[index, 'IN'] = '\n'.join(lista_nomes_escala)

    # Colocando aviso se existe pax cadeirante e hotel camboinha ou pitimbu

    df_in_reg_group['IN'] = df_in_reg_group.apply(ajustar_nomenclatura_cadeirante_regiao_hotel_in, axis=1)

    return df_in_reg_group[['Horario Apresentacao', 'IN', 'Total ADT | CHD', 'Roteiro', 'Carros', 'Id_Servico']]

def ajustar_nomenclatura_cadeirante_bara_regiao_hotel_in(row):

    info = row['IN']

    if row['Cadeirante']!='':

        info += "\nAtenção Paxs Cadeirante"

    if row['Est Origem'] == 'BA´RA HOTEL':

        info += "\nAtenção Paxs VIPs BARA"

    if row['Região Hotel']!='':

        info += f"\nAtenção HOTEL {row['Região Hotel']}"

    return info

def gerar_in_pvt(df_in):

    df_in_pvt_group = df_in[df_in['Modo do Servico']!='REGULAR']

    if len(df_in_pvt_group)>0:

        df_in_pvt_group['IN'] = 'PRIVATIVO | TRF IN ' +  df_in_pvt_group['Região IN'] + ' | ' + df_in_pvt_group['Voo | Horario Voo'].str[:15] + ' | ' + \
            df_in_pvt_group['Total ADT | CHD'].astype(int).astype(str) + ' PAXS\nReserva: ' + df_in_pvt_group['Reserva']
        
        # Inserindo coluna de Região Hotel
        
        df_in_pvt_group = pd.merge(df_in_pvt_group, st.session_state.df_hoteis_pitimbu_camboinha, left_on='Est Origem', right_on='Hoteis', how='left')
        
        # Colocando aviso se existe pax cadeirante

        df_in_pvt_group['IN'] = df_in_pvt_group.apply(ajustar_nomenclatura_cadeirante_bara_regiao_hotel_in, axis=1)
        
        return df_in_pvt_group[['Horario Apresentacao', 'IN', 'Total ADT | CHD', 'Roteiro', 'Carros']]
    
    else:

        return df_in_pvt_group

def juntar_reg_pvt_in(df_in_reg_group, df_in_pvt_group):

    df_in_final = pd.concat([df_in_reg_group, df_in_pvt_group], ignore_index=True)

    df_in_dia = df_in_final[df_in_final['Horario Apresentacao']>time(4)].sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_in_madrugada = df_in_final[df_in_final['Horario Apresentacao']<=time(4)].sort_values(by='Horario Apresentacao').reset_index(drop=True)

    df_in_final = pd.concat([df_in_dia, df_in_madrugada], ignore_index=True)

    df_in_final = df_in_final.rename(columns={'Total ADT | CHD': 'Paxs IN'})

    return df_in_final

def inserir_dados_gdrive(df_previa, aba_excel, id_gsheet):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(aba_excel)

    sheet.batch_clear(["A2:Z1000"])

    data = df_previa.values.tolist()
    start_cell = f"A2"
    
    sheet.update(start_cell, data)

    st.success(f'Dados inseridos no Drive com sucesso!')

def montar_linha_insercao(selected_rows_in_linha_total):

    linha_insercao = selected_rows_in_linha_total[['Horario Apresentacao', 'IN', 'Paxs IN', 'Id_Servico_IN']]

    linha_insercao[['Passeios | OUT', 'Paxs Passeios | OUT']] = ['', 0]

    linha_insercao['Horario Apresentacao'] = pd.to_datetime(linha_insercao['Horario Apresentacao']).dt.time

    return linha_insercao

def incluir_trf_in_conjugado(selected_rows_out, selected_rows_in, selected_rows_in_linha_total):

    # Preenche as colunas IN, Paxs IN e Id_Servico_IN

    st.session_state.df_tt_out.loc[selected_rows_out, ['IN', 'Paxs IN', 'Id_Servico_IN']] = selected_rows_in

    # Retira linha de st.session_state.df_in

    st.session_state.df_in = st.session_state.df_in.drop(index=int(selected_rows_in_linha_total['index'].iloc[0])).reset_index(drop=True)

    # Insere linha nos registros de trf in, pra poder voltar pra lista de trf in depois

    st.session_state.df_in_controle = pd.concat([st.session_state.df_in_controle, selected_rows_in_linha_total[['Horario Apresentacao', 'IN', 'Paxs IN', 'Id_Servico_IN']]], ignore_index=True)

    st.rerun()

def retirar_trf_in_previa(selected_rows_out):

    # Capta os valores de IN e Paxs IN

    info_in = st.session_state.df_tt_out.at[selected_rows_out, 'IN']

    paxs_in = st.session_state.df_tt_out.at[selected_rows_out, 'Paxs IN']

    id_servicos_in = st.session_state.df_tt_out.at[selected_rows_out, 'Id_Servico_IN']

    # Identificando se na linha realmente tem um trf in

    if 'TRF IN' in info_in:

        # Identificando se a exclusão é de um encaixe ou de uma linha só com TRF IN

        passeio_out = st.session_state.df_tt_out.at[selected_rows_out, 'Passeios | OUT']

        # Identifica qual o index da linha do df_in_controle que deve voltar pra tabela de trf in

        index_df_in = st.session_state.df_in_controle.loc[(st.session_state.df_in_controle['Id_Servico_IN']==id_servicos_in)].index.values[0]

        # Insere a linha de volta

        st.session_state.df_in = pd.concat([st.session_state.df_in, st.session_state.df_in_controle.iloc[[index_df_in]]], ignore_index=True)

        # Exclui a linha do controle

        st.session_state.df_in_controle = st.session_state.df_in_controle.drop(index=index_df_in).reset_index(drop=True)

        if passeio_out!='':

            # Apaga infos de IN e Paxs IN da tabela de OUT e Passeios

            st.session_state.df_tt_out.at[selected_rows_out, 'IN'] = ''

            st.session_state.df_tt_out.at[selected_rows_out, 'Paxs IN'] = ''

            st.session_state.df_tt_out.at[selected_rows_out, 'Id_Servico_IN'] = ''

        else:

            # Exclui a linha da tabela de passeios e outs

            st.session_state.df_tt_out = st.session_state.df_tt_out.drop(index=selected_rows_out).reset_index(drop=True)

        st.rerun()

    else:

        st.error('Na linha selecionada não tem nenhum TRF IN')

def encaixar_trf_in_nao_conjugado_na_ordem(df_tt_out_dia, linha_insercao):

    pos = df_tt_out_dia['Horario Apresentacao'].searchsorted(linha_insercao['Horario Apresentacao'])[0]

    # Divide o dataframe e depois concatena tudo colocando na ordem correta

    df1 = df_tt_out_dia.iloc[:pos]

    df2 = df_tt_out_dia.iloc[pos:]

    df_tt_out_dia = pd.concat([df1, linha_insercao, df2]).reset_index(drop=True)

    return df_tt_out_dia 

def separar_passeios_transfer_dia_madrugada():

    df_tt = st.session_state.df_tt_out[(~st.session_state.df_tt_out['Passeios | OUT'].str.contains('TRF OUT')) & (st.session_state.df_tt_out['Passeios | OUT']!='')]

    df_out_dia = st.session_state.df_tt_out[(st.session_state.df_tt_out['Horario Apresentacao']>time(4)) & 
                                            ((st.session_state.df_tt_out['Passeios | OUT'].str.contains('TRF OUT')) | (st.session_state.df_tt_out['Passeios | OUT']==''))]

    df_out_madrugada = st.session_state.df_tt_out[(st.session_state.df_tt_out['Horario Apresentacao']<=time(4)) & 
                                                ((st.session_state.df_tt_out['Passeios | OUT'].str.contains('TRF OUT')) | (st.session_state.df_tt_out['Passeios | OUT']==''))]

    return df_tt, df_out_dia, df_out_madrugada

def montando_df_tt_out_na_ordem(df_tt, df_out_dia, df_out_madrugada):

    st.session_state.df_tt_out = pd.concat([df_tt, df_out_dia]).reset_index(drop=True)

    st.session_state.df_tt_out = st.session_state.df_tt_out.sort_values(by=['Horario Apresentacao'])

    st.session_state.df_tt_out = pd.concat([st.session_state.df_tt_out, df_out_madrugada]).reset_index(drop=True)

def ajustar_horario_trf_in_nao_conjugado(linha_insercao):

    horario_original = linha_insercao.at[0, 'Horario Apresentacao']

    servico = linha_insercao.at[0, 'IN']

    if 'IN JPA' in servico:

        intervalo = timedelta(hours=1, minutes=30)

    else:

        intervalo = timedelta(hours=3, minutes=30)

    horario_como_datetime = datetime.combine(datetime.today(), horario_original)

    horario_ajustado = (horario_como_datetime - intervalo).time()

    linha_insercao.at[0, 'Horario Apresentacao'] = horario_ajustado

    return linha_insercao

def incluir_trf_nao_conjugado(selected_rows_in_linha_total):

    # Monta a linha que vai ser inserida no dataframe de passeios e OUT

    linha_insercao = montar_linha_insercao(selected_rows_in_linha_total)

    # Ajusta o horário da escala pra 1:30 antes do horário do voo

    linha_insercao = ajustar_horario_trf_in_nao_conjugado(linha_insercao)

    # Divide os passeio e outs do dia e madrugada

    df_tt, df_out_dia, df_out_madrugada = separar_passeios_transfer_dia_madrugada()

    # Identifica se a linha é na madrugada ou no dia

    if linha_insercao['Horario Apresentacao'].iloc[0]>time(4,0):

        df_out_dia = encaixar_trf_in_nao_conjugado_na_ordem(df_out_dia, linha_insercao)

    else:

        df_out_madrugada = encaixar_trf_in_nao_conjugado_na_ordem(df_out_madrugada, linha_insercao)

    montando_df_tt_out_na_ordem(df_tt, df_out_dia, df_out_madrugada)

    # Retira linha de st.session_state.df_in

    st.session_state.df_in = st.session_state.df_in.drop(index=int(selected_rows_in_linha_total['index'].iloc[0])).reset_index(drop=True)

    # Insere linha nos registros de trf in, pra poder voltar pra lista de trf in depois

    st.session_state.df_in_controle = pd.concat([st.session_state.df_in_controle, selected_rows_in_linha_total[['Horario Apresentacao', 'IN', 'Paxs IN', 'Id_Servico_IN']]], ignore_index=True)

    st.rerun() 

def criar_df_gdrive():

    df_gdrive = st.session_state.df_tt_out.reset_index(drop=True)

    df_gdrive['Paxs Passeios | OUT'] = df_gdrive['Paxs Passeios | OUT'].astype(int).astype(str).replace('0', '')

    df_gdrive['Horario Apresentacao'] = df_gdrive['Horario Apresentacao'].apply(lambda x: x.strftime('%H:%M') if pd.notnull(x) else None)

    df_gdrive['Paxs IN'] = df_gdrive['Paxs IN'].astype(str)

    df_gdrive['Id_Servico_Passeios_OUT'] = df_gdrive['Id_Servico_Passeios_OUT'].fillna('').astype(str)

    df_gdrive['Id_Servico_IN'] = df_gdrive['Id_Servico_IN'].astype(str)

    df_gdrive[['Veiculo', 'Cap.', 'Reb.', 'Rt.', 'Motorista', 'Guia']] = ''

    df_gdrive = df_gdrive[['Horario Apresentacao', 'Veiculo', 'Cap.', 'Reb.', 'Rt.', 'Motorista', 'Guia', 'Passeios | OUT', 'IN', 'Paxs Passeios | OUT', 'Paxs IN', 'Id_Servico_Passeios_OUT', 
                           'Id_Servico_IN']]

    return df_gdrive

def plotar_tabela_trf_in():

    gb_in = GridOptionsBuilder.from_dataframe(st.session_state.df_in)
    gb_in.configure_selection('single')
    gb_in.configure_grid_options(domLayout='autoHeight')
    gridOptions = gb_in.build()

    grid_response_in = AgGrid(st.session_state.df_in, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True)

    if not grid_response_in['selected_rows'] is None:

        selected_rows_in = [grid_response_in['selected_rows'].reset_index()['IN'].iloc[0], grid_response_in['selected_rows'].reset_index()['Paxs IN'].iloc[0], 
                            grid_response_in['selected_rows'].reset_index()['Id_Servico_IN'].iloc[0]]

        selected_rows_in_linha_total = grid_response_in['selected_rows'].reset_index()

    else:

        selected_rows_in = None

        selected_rows_in_linha_total = None

    return selected_rows_in, selected_rows_in_linha_total

def plotar_tabela_trf_out_passeios():

    row_height = 32
    header_height = 56  
    num_rows = len(st.session_state.df_tt_out)
    height = header_height + (row_height * num_rows)  

    gb = GridOptionsBuilder.from_dataframe(st.session_state.df_tt_out)
    gb.configure_selection('single')
    gb.configure_grid_options()
    gridOptions = gb.build()

    grid_response_out = AgGrid(st.session_state.df_tt_out, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True, height=height)

    if not grid_response_out['selected_rows'] is None:

        selected_rows_out = int(grid_response_out['selected_rows'].reset_index()['index'].iloc[0])

    else:

        selected_rows_out = None

    return selected_rows_out

def puxar_previa_de_escala(id_gsheet, nome_df, aba):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def identificar_regiao_trf(servico_out):

    if 'JPA' in servico_out:

        return 'AEROPORTO JOÃO PESSOA'

    elif 'REC' in servico_out:

        return 'AEROPORTO RECIFE'

    elif 'NAT' in servico_out:

        return 'AEROPORTO NATAL'

    elif 'CPV' in servico_out:

        return 'AEROPORTO CAMPINA GRANDE'

def gerar_payload_criacao_escalas(data_roteiro):

        lista_payload = []

        for index, row in st.session_state.df_previa.iterrows():

            date_str = data_roteiro.strftime('%Y-%m-%d')

            if row['Guia']!='':

                if row['Id_Servico_Passeios_OUT']!='':

                    payload = {"date": date_str, "vehicle_id": row['id_veiculo'], "driver_id": row['id_motorista'], "guide_id": row['id_guia'], 
                            "reserve_service_ids": row['Id_Servico_Passeios_OUT'].split(', ')}

                    lista_payload.append(payload)

                if row['Id_Servico_IN']!='':

                    payload = {"date": date_str, "vehicle_id": row['id_veiculo'], "driver_id": row['id_motorista'], "guide_id": row['id_guia'], 
                            "reserve_service_ids": row['Id_Servico_IN'].split(', ')}

                    lista_payload.append(payload)

            else:

                if row['Id_Servico_Passeios_OUT']!='':

                    payload = {"date": date_str, "vehicle_id": row['id_veiculo'], "driver_id": row['id_motorista'], "reserve_service_ids": row['Id_Servico_Passeios_OUT'].split(', ')}

                    lista_payload.append(payload)

                if row['Id_Servico_IN']!='':

                    payload = {"date": date_str, "vehicle_id": row['id_veiculo'], "driver_id": row['id_motorista'], "reserve_service_ids": row['Id_Servico_IN'].split(', ')}

                    lista_payload.append(payload)

        return lista_payload

def update_scale(payload):

    try:
        response = requests.post(st.session_state.base_url_post, json=payload, verify=False)
        response.raise_for_status()
        return 'Escala atualizada com sucesso!'
    except requests.RequestException as e:
        st.error(f"Ocorreu um erro: {e}")
        return 'Erro ao atualizar a escala'

def retirar_voo_g3_0001(df_in_final, df_tt_out):

    mask_retirar_voo_g30001_reg_in = (~df_in_final['IN'].str.contains('G3 - 0001')) & (~df_in_final['IN'].str.contains('PRIVATIVO'))

    df_in_final = df_in_final[mask_retirar_voo_g30001_reg_in].reset_index(drop=True)

    mask_retirar_voo_g30001_reg_out = (~df_tt_out['Passeios | OUT'].str.contains('G3 - 0001')) & (~df_tt_out['Passeios | OUT'].str.contains('PRIVATIVO'))

    df_tt_out = df_tt_out[mask_retirar_voo_g30001_reg_out].reset_index(drop=True)

    return df_in_final, df_tt_out

def gerar_listas_de_nao_cadastrados(df, coluna):

    if coluna=='Guia':

        lista_a_atualizar = st.session_state.df_previa[st.session_state.df_previa[coluna]!=''][coluna].unique().tolist()

    else:

        lista_a_atualizar = st.session_state.df_previa[coluna].unique().tolist()

    lista_phoenix = st.session_state[df][coluna].unique().tolist()

    lista_nao_cadastrados = list(set(lista_a_atualizar) - set(lista_phoenix))

    return lista_nao_cadastrados

def gerar_mensagens_de_nao_cadastrados(lista_veiculos_nao_cadastrados, lista_motoristas_nao_cadastrados, lista_guias_nao_cadastrados):

    if len(lista_veiculos_nao_cadastrados)>0:

        st.error(f'Os veículos {", ".join(lista_veiculos_nao_cadastrados)} não existem no Phoenix. Precisa ajustar a nomenclatura na planilha e tentar novamente')

    if len(lista_motoristas_nao_cadastrados)>0:

        st.error(f'Os motoristas {", ".join(lista_motoristas_nao_cadastrados)} não existem no Phoenix. Precisa ajustar a nomenclatura na planilha e tentar novamente')

    if len(lista_guias_nao_cadastrados)>0:

        st.error(f'Os guias {", ".join(lista_guias_nao_cadastrados)} não existem no Phoenix. Precisa ajustar a nomenclatura na planilha e tentar novamente')

    if len(lista_veiculos_nao_cadastrados)>0 or len(lista_motoristas_nao_cadastrados)>0 or len(lista_guias_nao_cadastrados)>0:

        st.stop()

def verificar_cadastros_veic_mot_guias():

    lista_veiculos_nao_cadastrados = gerar_listas_de_nao_cadastrados('df_veiculos', 'Veículo')

    lista_motoristas_nao_cadastrados = gerar_listas_de_nao_cadastrados('df_motoristas', 'Motorista')

    lista_guias_nao_cadastrados = gerar_listas_de_nao_cadastrados('df_guias', 'Guia')

    gerar_mensagens_de_nao_cadastrados(lista_veiculos_nao_cadastrados, lista_motoristas_nao_cadastrados, lista_guias_nao_cadastrados)

def incluir_ids_veiculos_motoristas_guias():

    st.session_state.df_previa = pd.merge(st.session_state.df_previa, st.session_state.df_veiculos[['Veículo', 'id_veiculo']], on='Veículo', how='left')

    st.session_state.df_previa = pd.merge(st.session_state.df_previa, st.session_state.df_guias[['Guia', 'id_guia']], on='Guia', how='left')

    st.session_state.df_previa = pd.merge(st.session_state.df_previa, st.session_state.df_motoristas[['Motorista', 'id_motorista']], on='Motorista', how='left')

st.set_page_config(layout='wide')

if not 'vw_atual' in st.session_state:

    st.session_state.vw_atual = 'vw_previa'

if not 'primeira_previa' in st.session_state:

    st.session_state.primeira_previa = True

    st.session_state.titulo = 'Prévia de Escala - João Pessoa'

    st.session_state.id_gsheet = '1vbGeqKyM4VSvHbMiyiqu1mkwEhneHi28e8cQ_lYMYhY'

    st.session_state.base_url_get = 'https://driverjoao_pessoa.phoenix.comeialabs.com/scale/'

    st.session_state.base_url_post = 'https://driverjoao_pessoa.phoenix.comeialabs.com/scale/roadmap/allocate'

if not 'df_in_controle' in st.session_state:

    st.session_state.df_in_controle = pd.DataFrame(columns=['Horario Apresentacao', 'IN', 'Paxs IN', 'Id_Servico_IN'])

if not 'df_router' in st.session_state or st.session_state.vw_atual != 'vw_previa':

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

st.title(st.session_state.titulo)

st.divider()

row1=st.columns(3)

with row1[0]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    if atualizar_phoenix:

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

    container_roteirizar = st.container(border=True)

    data_roteiro = container_roteirizar.date_input('Data do Roteiro', value=None, format='DD/MM/YYYY', key='data_roteiro')

    gerar_layout = container_roteirizar.button('Gerar Layout')

with row1[1]:

    escalar_trf_privativos = st.button('Escalar TRF IN e OUT')

    if escalar_trf_privativos:

        with st.spinner('Puxando Prévia do Google Drive...'):

            puxar_previa_de_escala(st.session_state.id_gsheet, 'df_previa', 'Prévia Escala')

            incluir_ids_veiculos_motoristas_guias()

        verificar_cadastros_veic_mot_guias()

        lista_payload = gerar_payload_criacao_escalas(data_roteiro)

        with st.spinner('Criando escalas no Phoenix...'):

            for escala in lista_payload:

                status = update_scale(escala)

st.divider()

if gerar_layout:

    # Puxando histórico de roteiros

    with st.spinner('Puxando roteiros de IN e OUT, pontos de apoio, agenda de embarques, nomes de operadoras, hoteis camboinha/pitimbu...'):

        puxar_historico_roteiros_apoios(st.session_state.id_gsheet, 'df_historico_roteiros', 'Histórico Roteiros', 'df_pontos_de_apoio', 'Pontos de Apoio', 'df_embarques', 'Agenda Embarques', 
                                        'df_operadoras', 'Nomes Operadoras', 'df_hoteis_pitimbu_camboinha', 'Hoteis Camboinha | Pitimbu')

    df_router_filtrado = criar_df_router_filtrado()

    # Verificando se todos os serviços IN e OUT foram roteirizados

    verificar_servicos_in_out_sem_roteiros(df_router_filtrado)

    # Criando df com TOUR e TRANSFER privativo

    df_tt_pvt = criar_df_tt_pvt_final(df_router_filtrado)

    # Criando df com TOUR e TRANSFER regular

    df_tt_reg = criar_df_tt_reg_final(df_router_filtrado)

    # Criando df com TOUR e TRANSFER privativo e regular

    df_tt = criar_df_tt_final(df_tt_pvt, df_tt_reg)

    # Subindo 'EMBARCAÇÃO -' lá pra cima da prévia

    df_tt = colocar_embarcacoes_no_topo(df_tt)

    # Colocando Pontos de Apoio de cada passeio

    df_tt = pd.merge(df_tt, st.session_state.df_pontos_de_apoio, on='Nome Original Servico', how='left')

    # Colocando horários de embarque se houver ILHA ou EXTREMO

    df_tt = pd.merge(df_tt, st.session_state.df_embarques[st.session_state.df_embarques['Data Execucao']==data_roteiro][['Nome Original Servico', 'Embarque']], on='Nome Original Servico', how='left')

    # Verificar se tem Ilha ou Extremo e se existe embarque cadastrado pra data escolhida

    verificar_embarques(df_tt)

    # Juntar os tipos de Litoral Norte

    df_tt = juntar_paxs_litoral_norte(df_tt)

    # Colocar Deixar Quadrilha na escala quando tem catamarã do forró

    df_tt = inserir_deixar_quadrilha(df_tt)

    # Aumentando quantidade de veículos nos trilhas se for mais que 9 paxs

    df_tt = identificando_quantidade_carros_trilhas(df_tt, 9)

    # Ajustando nomes de passeios com Pontos de Apoio e Embarques

    df_tt['Passeios | OUT'] = df_tt.apply(adicionar_embarque_ponto_de_apoio, axis=1)

    # Retirando colunas que não vou mais usar

    df_tt = df_tt.drop(columns=['Ponto de Apoio', 'Embarque', 'Nome Original Servico'])

    # Tranformando coluna Id_Servico em string pra quando juntar com os OUTs e INs tudo estar no mesmo formato

    df_tt['Id_Servico'] = df_tt['Id_Servico'].fillna(0).astype(int).astype(str).replace('0', '')

    # Criado df com OUTs do dia e madrugada

    df_out = criar_df_out(df_router_filtrado)

    # Inserir definição de roteiros e carros

    df_out = pd.merge(df_out, st.session_state.df_historico_roteiros[['Data Execucao', 'Id_Servico', 'Roteiro', 'Carros']], on=['Data Execucao', 'Id_Servico'], how='left')

    # Avisar se tiver reserva regular no voo G3 - 0001

    avisar_reg_g3_0001(df_out)

    # Ajustando nomes de operadoras

    df_out = ajustar_nomes_operadoras(df_out)

    # Agrupando roteiros e carros de trf OUT regular e gerando nome que vai pra escala

    df_out_reg_group = agrupar_roteiros_carros_nome_escala_out(df_out)

    # Gerando trf OUT privativos

    df_out_pvt_group = gerar_out_pvt(df_out)

    # Concatenando reg e pvt, ordenando e inserindo colunas que faltam pra concatenar com os passeios

    df_out_final = juntar_reg_pvt_out(df_out_reg_group, df_out_pvt_group)

    # Concatenando Passeios e OUT e ordenando dataframe

    df_tt_out = concat_tt_out_ordem_cronologica(df_tt, df_out_final)

    df_tt_out = df_tt_out.rename(columns={'Id_Servico': 'Id_Servico_Passeios_OUT'})

    # Criado df com INs do dia e madrugada

    df_in = criar_df_in(df_router_filtrado)

    # Inserir definição de roteiros e carros

    df_in = pd.merge(df_in, st.session_state.df_historico_roteiros[['Data Execucao', 'Id_Servico', 'Roteiro', 'Carros']], on=['Data Execucao', 'Id_Servico'], how='left')

    # Avisar se tiver reserva regular no voo G3 - 0001

    avisar_reg_g3_0001(df_in)

    # Ajustando nomes de operadoras

    df_in = ajustar_nomes_operadoras(df_in)

    # Agrupando roteiros e carros de trf IN regular e gerando nome que vai pra escala

    df_in_reg_group = agrupar_roteiros_carros_nome_escala_in(df_in)

    # Gerando trf IN privativos

    df_in_pvt_group = gerar_in_pvt(df_in)

    # Concatenando reg e pvt, ordenando e inserindo colunas que faltam pra concatenar com os passeios

    df_in_final = juntar_reg_pvt_in(df_in_reg_group, df_in_pvt_group)

    df_in_final = df_in_final.rename(columns={'Id_Servico': 'Id_Servico_IN'})

    # Retirando voo G3 - 0001 REGULAR da prévia

    df_in_final, df_tt_out = retirar_voo_g3_0001(df_in_final, df_tt_out)

    df_tt_out[['IN', 'Paxs IN', 'Id_Servico_IN']] = ''

    st.session_state.df_tt_out = df_tt_out[['Horario Apresentacao', 'Passeios | OUT', 'IN', 'Paxs Passeios | OUT', 'Paxs IN', 'Id_Servico_Passeios_OUT', 'Id_Servico_IN']]

    st.session_state.df_in = df_in_final[['Horario Apresentacao', 'IN', 'Paxs IN', 'Id_Servico_IN']]

if 'df_tt_out' in st.session_state:

    # Plotar tabela com TRF IN

    selected_rows_in, selected_rows_in_linha_total = plotar_tabela_trf_in()

    # Plotar tabela com Passeios e OUTs

    selected_rows_out = plotar_tabela_trf_out_passeios()

    row_botoes = st.columns(5)

    with row_botoes[0]:

        incluir_trf_in = st.button('Incluir TRF IN')

    with row_botoes[1]:

        excluir_trf_in = st.button('Excluir TRF IN')

    # Se for incluir trf in, e tiver selecionado uma linha de trf in e outra de trf out

    if incluir_trf_in and not selected_rows_out is None and not selected_rows_in is None:

        incluir_trf_in_conjugado(selected_rows_out, selected_rows_in, selected_rows_in_linha_total)

    # Se for tirar um TRF IN da prévia

    if excluir_trf_in and not selected_rows_out is None:

        retirar_trf_in_previa(selected_rows_out)

    # Se for incluir trf in e não tiver selecionado linha de passeio ou out

    if incluir_trf_in and selected_rows_out is None and not selected_rows_in is None:

        incluir_trf_nao_conjugado(selected_rows_in_linha_total)

    with row_botoes[2]:

        gerar_planilha = st.button('Gerar Planilha')

    if gerar_planilha:

        df_gdrive = criar_df_gdrive()

        if len(st.session_state.df_in)>0:

            st.error('Ainda existe TRF IN fora da prévia. Termine de inserí-los e tente novamente')

        else:

            inserir_dados_gdrive(df_gdrive, 'Prévia Escala', st.session_state.id_gsheet)
