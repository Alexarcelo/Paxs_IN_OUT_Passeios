import pandas as pd
import mysql.connector
import decimal
import streamlit as st
import matplotlib.pyplot as plt
import gspread 
from google.cloud import secretmanager 
import json
from google.oauth2.service_account import Credentials

def gerar_df_phoenix(vw_name, base_luck):
    # Parametros de Login AWS
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    # Conexão as Views
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT `Data Execucao`, `Tipo de Servico`, `Status do Servico`, `Status da Reserva`, `Servico`, `Total ADT`, `Total CHD`, `Parceiro`, `Observacao` FROM {vw_name}'

    # Script MySql para requests
    cursor.execute(
        request_name
    )
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas o cabecalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e muda o tipo de decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def puxar_dados_phoenix(base_luck):

    st.session_state.df_router_bruto = gerar_df_phoenix('vw_router', base_luck)

    st.session_state.filtrar_servicos_geral = []

    st.session_state.filtrar_servicos_geral.extend(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços IN'].tolist())))

    st.session_state.filtrar_servicos_geral.extend(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços TOUR'].tolist())))

    st.session_state.mapa_router = \
        st.session_state.df_router_bruto[(~st.session_state.df_router_bruto['Status do Servico'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status do Serviço'].tolist())))) & 
                                         (~st.session_state.df_router_bruto['Status da Reserva'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status da Reserva'].tolist())))) & 
                                         (~pd.isna(st.session_state.df_router_bruto[list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Colunas Vazias'].tolist()))]).any(axis=1)) &
                                         (~st.session_state.df_router_bruto['Servico'].isin(st.session_state.filtrar_servicos_geral))].reset_index(drop=True)

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def inserir_config(df_itens_faltantes, id_gsheet, nome_aba):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z100"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

    st.success('Configurações salvas com sucesso!')

def criar_colunas_ano_mes(df):

    df['ano'] = pd.to_datetime(df['Data Execucao']).dt.year

    df['mes'] = pd.to_datetime(df['Data Execucao']).dt.month

    return df

def ajustar_dataframe_group_mensal(df):

    df_group = df.groupby(['ano', 'mes'])[['Paxs Totais']].sum().reset_index()

    df_group = df_group[(df_group['mes']>=data_inicial.month) & (df_group['mes']<=data_final.month)].reset_index(drop=True)

    df_group['mes/ano'] = pd.to_datetime(df_group['ano'].astype(str) + '-' + df_group['mes'].astype(str)).dt.to_period('M')

    return df_group

def grafico_linha_numero(referencia, eixo_x, eixo_y_1, ref_1_label, titulo):

    referencia[eixo_x] = referencia[eixo_x].astype(str)
    
    fig, ax = plt.subplots(figsize=(15, 8))
    
    plt.plot(referencia[eixo_x], referencia[eixo_y_1], label = ref_1_label, linewidth = 0.5, color = 'black')
    
    for i in range(len(referencia[eixo_x])):
        texto = str(int(referencia[eixo_y_1][i]))
        plt.text(referencia[eixo_x][i], referencia[eixo_y_1][i], texto, ha='center', va='bottom')

    plt.title(titulo, fontsize=30)
    plt.xlabel('Ano/Mês')
    ax.legend(loc='lower right', bbox_to_anchor=(1.2, 1))
    st.pyplot(fig)
    plt.close(fig)


st.set_page_config(layout='wide')

st.session_state.id_sheet = '11MIhssCgpQKIwP3snUfXUOUbh8g716JZvj1TLP234iQ'

if not 'selecao_base_luck' in st.session_state:

    st.session_state.selecao_base_luck = None

if not 'mostrar_config' in st.session_state:

        st.session_state.mostrar_config = False

if not 'dict_bases' in st.session_state:

    st.session_state.dict_bases = {'Aracajú': ['test_phoenix_aracaju', 'Configurações Aracajú', 'Paxs IN, OUT e Passeios | Aracajú'], 
                                   'Natal': ['test_phoenix_natal', 'Configurações Natal', 'Paxs IN, OUT e Passeios | Natal'], 
                                   'João Pessoa': ['test_phoenix_joao_pessoa', 'Configurações João Pessoa', 'Paxs IN, OUT e Passeios | João Pessoa'], 
                                   'Maceió': ['test_phoenix_maceio', 'Configurações Maceió', 'Paxs IN, OUT e Passeios | Maceió'], 
                                   'Salvador': ['test_phoenix_salvador', 'Configurações Salvador', 'Paxs IN, OUT e Passeios | Salvador'], 
                                   'Recife': ['test_phoenix_recife', 'Configurações Recife', 'Paxs IN, OUT e Passeios | Recife'], 
                                   'Noronha': ['test_phoenix_noronha', 'Configurações Noronha', 'Paxs IN, OUT e Passeios | Noronha']}

row0 = st.columns(1)

with row0[0]:

    nome_base_luck = st.selectbox('Escolha a Base Luck', sorted(['Aracajú', 'Natal', 'João Pessoa', 'Maceió', 'Salvador', 'Recife', 'Noronha']), index=None, key='nome_base_luck')

if nome_base_luck!=st.session_state.selecao_base_luck:

    st.session_state.selecao_base_luck = nome_base_luck

    with st.spinner('Puxando configurações...'):

        puxar_aba_simples(st.session_state.id_sheet, st.session_state.dict_bases[nome_base_luck][1], 'df_config')

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix(st.session_state.dict_bases[nome_base_luck][0])

if nome_base_luck is not None:

    st.title(st.session_state.dict_bases[nome_base_luck][2])

    st.divider()

    st.header('Configurações')

    alterar_configuracoes = st.button('Visualizar Configurações')

    if alterar_configuracoes:

        if st.session_state.mostrar_config == True:

            st.session_state.mostrar_config = False

        else:

            st.session_state.mostrar_config = True

    row01 = st.columns(3)

    if st.session_state.mostrar_config == True:

        with row01[0]:

            filtrar_status_servico = st.multiselect('Excluir Status do Serviço', sorted(st.session_state.df_router_bruto['Status do Servico'].dropna().unique().tolist()), key='filtrar_status_servico', 
                                                    default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status do Serviço'].tolist())))

            filtrar_status_reserva = st.multiselect('Excluir Status da Reserva', sorted(st.session_state.df_router_bruto['Status da Reserva'].dropna().unique().tolist()), key='filtrar_status_reserva', 
                                                    default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status da Reserva'].tolist())))

        with row01[1]:
            
            filtrar_servicos_in = st.multiselect('Excluir Serviços IN', sorted(st.session_state.df_router_bruto[st.session_state.df_router_bruto['Tipo de Servico']=='IN']['Servico'].unique().tolist()), 
                                                key='filtrar_servicos_in', default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços IN'].tolist())))
            
            filtrar_servicos_tt = st.multiselect('Excluir Serviços TOUR', 
                                                sorted(st.session_state.df_router_bruto[st.session_state.df_router_bruto['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])]['Servico'].unique().tolist()), 
                                                key='filtrar_servicos_tt', default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços TOUR'].tolist())))
            
        with row01[2]:

            filtrar_colunas_vazias = st.multiselect('Não Permitir Valor Vazio', sorted(st.session_state.df_router_bruto.columns.tolist()), key='filtrar_colunas_vazias', 
                                                    default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Colunas Vazias'].tolist())))
            
            excluir_cld = st.multiselect('Excluir reservas com CLD na observação', ['Sim'], key='excluir_cld', 
                                        default=list(filter(lambda x: x != '', st.session_state.df_config['Excluir CLD'].tolist())))

            excluir_cortesia = st.multiselect('Excluir reservas com CORTESIA na observação', ['Sim'], key='excluir_cortesia', 
                                        default=list(filter(lambda x: x != '', st.session_state.df_config['Excluir CORTESIA'].tolist())))
            
            st.session_state.filtrar_servicos_geral = []

            st.session_state.filtrar_servicos_geral.extend(filtrar_servicos_in)

            st.session_state.filtrar_servicos_geral.extend(filtrar_servicos_tt)

        salvar_config = st.button('Salvar Configurações')

        if salvar_config:

            with st.spinner('Salvando Configurações...'):

                lista_escolhas = [filtrar_status_servico, filtrar_status_reserva, filtrar_colunas_vazias, filtrar_servicos_in, filtrar_servicos_tt, excluir_cld, excluir_cortesia]

                st.session_state.df_config = pd.DataFrame({f'Coluna{i+1}': pd.Series(lista) for i, lista in enumerate(lista_escolhas)})

                st.session_state.df_config = st.session_state.df_config.fillna('')

                inserir_config(st.session_state.df_config, st.session_state.id_sheet, st.session_state.dict_bases[nome_base_luck][1])

                puxar_aba_simples(st.session_state.id_sheet, st.session_state.dict_bases[nome_base_luck][1], 'df_config')

            st.session_state.mostrar_config = False

            st.rerun()

    st.divider()

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    if atualizar_phoenix:

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix(st.session_state.dict_bases[nome_base_luck][0])

    periodo = st.date_input('Período', value=[] ,format='DD/MM/YYYY')

    st.divider()

    if len(periodo)>1 and periodo[0].month == periodo[1].month and nome_base_luck:

        st.subheader('TRF IN')

        data_inicial = periodo[0]

        data_final = periodo[1]

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CLD'].tolist())))==1:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='IN') & 
                                                            (~st.session_state.mapa_router['Observacao'].str.upper().str.contains('CLD', na=False))].reset_index(drop=True)
            
        else:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='IN')].reset_index(drop=True)

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CORTESIA'].tolist())))==1:

            df_mapa_filtrado = df_mapa_filtrado[(~df_mapa_filtrado['Observacao'].str.upper().str.contains('CORTESIA', na=False))].reset_index(drop=True)
        
        df_mapa_filtrado['Paxs Totais'] = df_mapa_filtrado['Total ADT'] + df_mapa_filtrado['Total CHD']

        paxs_totais = int(df_mapa_filtrado['Paxs Totais'].sum())

        st.success(f'No período selecionado existem {paxs_totais} passageiros.')

        df_mapa_filtrado_group = df_mapa_filtrado.groupby('Servico')['Paxs Totais'].sum().reset_index()

        row1 = st.columns(2)

        with row1[0]:

            st.dataframe(df_mapa_filtrado_group.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        df_mapa_filtrado_group_parceiro = df_mapa_filtrado.groupby('Parceiro')['Paxs Totais'].sum().reset_index()

        with row1[1]:

            st.dataframe(df_mapa_filtrado_group_parceiro.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        st.divider()

        st.subheader('TRF OUT')

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CLD'].tolist())))==1:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='OUT') & 
                                                            (~st.session_state.mapa_router['Observacao'].str.upper().str.contains('CLD', na=False))].reset_index(drop=True)
            
        else:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='OUT')].reset_index(drop=True)

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CORTESIA'].tolist())))==1:

            df_mapa_filtrado = df_mapa_filtrado[(~df_mapa_filtrado['Observacao'].str.upper().str.contains('CORTESIA', na=False))].reset_index(drop=True)
        
        df_mapa_filtrado['Paxs Totais'] = df_mapa_filtrado['Total ADT'] + df_mapa_filtrado['Total CHD']

        paxs_totais = int(df_mapa_filtrado['Paxs Totais'].sum())

        st.success(f'No período selecionado existem {paxs_totais} passageiros.')

        df_mapa_filtrado_group = df_mapa_filtrado.groupby('Servico')['Paxs Totais'].sum().reset_index()

        row2 = st.columns(2)

        with row2[0]:

            st.dataframe(df_mapa_filtrado_group.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        df_mapa_filtrado_group_parceiro = df_mapa_filtrado.groupby('Parceiro')['Paxs Totais'].sum().reset_index()

        with row2[1]:

            st.dataframe(df_mapa_filtrado_group_parceiro.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        st.divider()

        st.subheader('Passeios')

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CLD'].tolist())))==1:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & 
                                                            (~st.session_state.mapa_router['Observacao'].str.upper().str.contains('CLD', na=False))].reset_index(drop=True)
            
        else:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico'].isin(['TOUR', 'TRANSFER']))].reset_index(drop=True)

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CORTESIA'].tolist())))==1:

            df_mapa_filtrado = df_mapa_filtrado[(~df_mapa_filtrado['Observacao'].str.upper().str.contains('CORTESIA', na=False))].reset_index(drop=True)
        
        df_mapa_filtrado['Paxs Totais'] = df_mapa_filtrado['Total ADT'] + df_mapa_filtrado['Total CHD']

        paxs_totais = int(df_mapa_filtrado['Paxs Totais'].sum())

        st.success(f'No período selecionado existem {paxs_totais} passageiros.')

        df_mapa_filtrado_group = df_mapa_filtrado.groupby('Servico')['Paxs Totais'].sum().reset_index()

        row3 = st.columns(2)

        with row3[1]:

            lista_servicos = sorted(df_mapa_filtrado_group['Servico'].unique().tolist())

            filtro_servicos = st.multiselect('Filtrar Serviços', lista_servicos, default=None)

        if filtro_servicos:

            df_mapa_filtrado_group = df_mapa_filtrado_group[df_mapa_filtrado_group['Servico'].isin(filtro_servicos)].reset_index(drop=True)

        with row3[0]:

            st.dataframe(df_mapa_filtrado_group.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

    elif len(periodo)>1 and periodo[0].month != periodo[1].month and nome_base_luck:

        st.subheader('TRF IN')

        data_inicial = periodo[0]

        data_final = periodo[1]

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CLD'].tolist())))==1:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='IN') & 
                                                            (~st.session_state.mapa_router['Observacao'].str.upper().str.contains('CLD', na=False))].reset_index(drop=True)
            
        else:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='IN')].reset_index(drop=True)

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CORTESIA'].tolist())))==1:

            df_mapa_filtrado = df_mapa_filtrado[(~df_mapa_filtrado['Observacao'].str.upper().str.contains('CORTESIA', na=False))].reset_index(drop=True)
        
        df_mapa_filtrado['Paxs Totais'] = df_mapa_filtrado['Total ADT'] + df_mapa_filtrado['Total CHD']

        paxs_totais = int(df_mapa_filtrado['Paxs Totais'].sum())

        st.success(f'No período selecionado existem {paxs_totais} passageiros.')

        df_mapa_filtrado_group = df_mapa_filtrado.groupby('Servico')['Paxs Totais'].sum().reset_index()

        row1 = st.columns(2)

        with row1[0]:

            st.dataframe(df_mapa_filtrado_group.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        df_mapa_filtrado_group_parceiro = df_mapa_filtrado.groupby('Parceiro')['Paxs Totais'].sum().reset_index()

        with row1[1]:

            st.dataframe(df_mapa_filtrado_group_parceiro.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        df_mapa_filtrado = criar_colunas_ano_mes(df_mapa_filtrado)

        df_group_mensal = ajustar_dataframe_group_mensal(df_mapa_filtrado)

        row2 = st.columns(1)

        with row2[0]:

            grafico_linha_numero(df_group_mensal, 'mes/ano', 'Paxs Totais', 'Paxs', 'TRF IN')

        st.divider()

        st.subheader('TRF OUT')

        data_inicial = periodo[0]

        data_final = periodo[1]

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CLD'].tolist())))==1:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='OUT') & 
                                                            (~st.session_state.mapa_router['Observacao'].str.upper().str.contains('CLD', na=False))].reset_index(drop=True)
            
        else:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico']=='OUT')].reset_index(drop=True)

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CORTESIA'].tolist())))==1:

            df_mapa_filtrado = df_mapa_filtrado[(~df_mapa_filtrado['Observacao'].str.upper().str.contains('CORTESIA', na=False))].reset_index(drop=True)
        
        df_mapa_filtrado['Paxs Totais'] = df_mapa_filtrado['Total ADT'] + df_mapa_filtrado['Total CHD']

        paxs_totais = int(df_mapa_filtrado['Paxs Totais'].sum())

        st.success(f'No período selecionado existem {paxs_totais} passageiros.')

        df_mapa_filtrado_group = df_mapa_filtrado.groupby('Servico')['Paxs Totais'].sum().reset_index()

        row1 = st.columns(2)

        with row1[0]:

            st.dataframe(df_mapa_filtrado_group.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        df_mapa_filtrado_group_parceiro = df_mapa_filtrado.groupby('Parceiro')['Paxs Totais'].sum().reset_index()

        with row1[1]:

            st.dataframe(df_mapa_filtrado_group_parceiro.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        df_mapa_filtrado = criar_colunas_ano_mes(df_mapa_filtrado)

        df_group_mensal = ajustar_dataframe_group_mensal(df_mapa_filtrado)

        row2 = st.columns(1)

        with row2[0]:

            grafico_linha_numero(df_group_mensal, 'mes/ano', 'Paxs Totais', 'Paxs', 'TRF OUT')

        st.divider()

        st.subheader('Passeios')

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CLD'].tolist())))==1:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & 
                                                            (~st.session_state.mapa_router['Observacao'].str.upper().str.contains('CLD', na=False))].reset_index(drop=True)
            
        else:

            df_mapa_filtrado = st.session_state.mapa_router[(st.session_state.mapa_router['Data Execucao'] >= data_inicial) & (st.session_state.mapa_router['Data Execucao'] <= data_final) & 
                                                            (st.session_state.mapa_router['Tipo de Servico'].isin(['TOUR', 'TRANSFER']))].reset_index(drop=True)

        if len(list(filter(lambda x: x != '', st.session_state.df_config['Excluir CORTESIA'].tolist())))==1:

            df_mapa_filtrado = df_mapa_filtrado[(~df_mapa_filtrado['Observacao'].str.upper().str.contains('CORTESIA', na=False))].reset_index(drop=True)
        
        df_mapa_filtrado['Paxs Totais'] = df_mapa_filtrado['Total ADT'] + df_mapa_filtrado['Total CHD']

        paxs_totais = int(df_mapa_filtrado['Paxs Totais'].sum())

        st.success(f'No período selecionado existem {paxs_totais} passageiros.')

        df_mapa_filtrado_group = df_mapa_filtrado.groupby('Servico')['Paxs Totais'].sum().reset_index()

        row3 = st.columns(2)

        with row3[1]:

            lista_servicos = sorted(df_mapa_filtrado_group['Servico'].unique().tolist())

            filtro_servicos = st.multiselect('Filtrar Serviços', lista_servicos, default=None)

        if filtro_servicos:

            df_mapa_filtrado_group = df_mapa_filtrado_group[df_mapa_filtrado_group['Servico'].isin(filtro_servicos)].reset_index(drop=True)

            df_mapa_filtrado = df_mapa_filtrado[df_mapa_filtrado['Servico'].isin(filtro_servicos)].reset_index(drop=True)

        with row3[0]:

            st.dataframe(df_mapa_filtrado_group.sort_values(by='Paxs Totais', ascending=False), hide_index=True)

        df_mapa_filtrado = criar_colunas_ano_mes(df_mapa_filtrado)

        df_group_mensal = ajustar_dataframe_group_mensal(df_mapa_filtrado)

        row2 = st.columns(1)

        with row2[0]:

            grafico_linha_numero(df_group_mensal, 'mes/ano', 'Paxs Totais', 'Paxs', 'Passeios')
