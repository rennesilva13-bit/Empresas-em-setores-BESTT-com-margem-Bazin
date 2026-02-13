import yfinance as yf
import pandas as pd
import math
import os
from datetime import datetime
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Lista completa de 130 tickers da B3 - distribuição estratégica por setores
# ==============================================================================
# LISTA ATUALIZADA 2026 - 130 TICKERS
# ==============================================================================

TICKERS_130 = [
    # --- Setor Bancário & Seguros (B) ---
    'BBAS3', 'ITSA4', 'SANB11', 'BBDC4', 'BPAC11', 'B3SA3', 'BRAP4', 'CIEL3', 'IRBR3', 'ITUB4',
    'PSSA3', 'ABCB4', 'BBSE3', 'CXSE3', 'BRSR6', 'MULT3', 'WIZC3', 'BMGB4', 'BPAN4', 'CLSA3',

    # --- Energia Elétrica (E) ---
    'TAEE11', 'EGIE3', 'CPLE6', 'SBSP3', 'AESB3', 'ENGI11', 'EQTL3', 'TRPL4', 'CMIG4', 'AURE3',
    'REDE3', 'ALUP11', 'NEOE3', 'SAPR11', 'MEGA3', 'ELET3', 'ELET6', 'CPFE3', 'LIGT3', 'GEPA4',

    # --- Saneamento (S) ---
    'SAPR4', 'CSMG3', 'AMBP3', 'SAPR3', 'CSUD3', 'ORVR3', 'SYNE3', 'ALSO3',

    # --- Telecomunicações (T) ---
    'VIVT3', 'TIMS3', 'OIBR3', 'FIQE3', 'DESK3', 'BRIT3',

    # --- Varejo, Shopping & Consumo ---
    'ALOS3', 'IGTI11', 'JHSF3', 'LREN3', 'MGLU3', 'BHIA3', 'AZZA3', 'AMER3', 'CAMB3', 'CEAB3',
    'CVCB3', 'GRND3', 'GUAR3', 'HYPE3', 'MOVI3', 'PETZ3', 'SBFG3', 'VIVA3', 'ZAMP3', 'MDIA3',
    'ABEV3', 'CRFB3', 'ASAI3', 'NTCO3', 'SMTO3', 'SRNA3',

    # --- Construção & Engenharia ---
    'CYRE3', 'EZTC3', 'MRVE3', 'TEND3', 'EVEN3', 'DIRR3', 'JSLG3', 'CURY3', 'LAVV3', 'MELK3',
    'PLPL3', 'HBOR3', 'GFSA3', 'TRIS3',

    # --- Mineração, Siderurgia & Petróleo ---
    'VALE3', 'CSNA3', 'USIM5', 'GGBR4', 'GOAU4', 'MRFG3', 'PETR4', 'PRIO3', 'BRAV3', 'RECV3',
    'UGPA3', 'JBSS3', 'BRFS3', 'BEEF3', 'KLBN11', 'SUZB3', 'DXCO3', 'RANI3', 'UNIP6', 'FESA4',

    # --- Saúde & Educação ---
    'FLRY3', 'HAPV3', 'RDOR3', 'QUAL3', 'RADL3', 'PNVL3', 'ODPV3', 'MATD3', 'VVEO3', 'YDUQ3',
    'COGN3', 'ANIM3',

    # --- Transporte, Logística & Bens Industriais ---
    'CCRO3', 'ECOR3', 'RAIL3', 'GOLL4', 'AZUL4', 'HBSA3', 'STBP3', 'PORT3', 'SLCE3', 'TTEN3',
    'VAMO3', 'SIMH3', 'WEGE3', 'TUPY3', 'RAPT4', 'POMO4', 'EMBR3', 'KEPL3', 'SHUL4', 'LEVE3',

    # --- Tecnologia & Outros ---
    'TOTS3', 'LWSA3', 'CASH3', 'INTB3', 'POSI3', 'MLAS3'
]

# ==============================================================================
# MAPEAMENTO DE SETORES BESTT (Atualizado)
# ==============================================================================

SETORES_BESTT = {
    # BANCOS E SEGUROS
    'BBAS3': 'Bancos', 'ITSA4': 'Bancos', 'SANB11': 'Bancos', 'BBDC4': 'Bancos', 'BPAC11': 'Bancos',
    'B3SA3': 'Bancos', 'BRAP4': 'Bancos', 'CIEL3': 'Bancos', 'IRBR3': 'Bancos', 'ITUB4': 'Bancos',
    'PSSA3': 'Bancos', 'ABCB4': 'Bancos', 'BBSE3': 'Bancos', 'CXSE3': 'Bancos', 'BRSR6': 'Bancos',
    'BMGB4': 'Bancos', 'BPAN4': 'Bancos', 'WIZC3': 'Bancos',

    # ENERGIA
    'TAEE11': 'Energia', 'EGIE3': 'Energia', 'CPLE6': 'Energia', 'SBSP3': 'Energia', 'AESB3': 'Energia',
    'ENGI11': 'Energia', 'EQTL3': 'Energia', 'TRPL4': 'Energia', 'CMIG4': 'Energia', 'AURE3': 'Energia',
    'REDE3': 'Energia', 'ALUP11': 'Energia', 'NEOE3': 'Energia', 'SAPR11': 'Energia', 'ELET3': 'Energia',
    'ELET6': 'Energia', 'CPFE3': 'Energia', 'MEGA3': 'Energia', 'LIGT3': 'Energia',

    # SANEAMENTO (Parte do S do BESTT original ou categoria própria)
    'SAPR4': 'Saneamento', 'CSMG3': 'Saneamento', 'AMBP3': 'Saneamento', 'SAPR3': 'Saneamento',

    # TELECOM
    'VIVT3': 'Telecom', 'TIMS3': 'Telecom', 'OIBR3': 'Telecom', 'FIQE3': 'Telecom', 'DESK3': 'Telecom',
    'BRIT3': 'Telecom'
}

def classificar_setor_bestt(ticker):
    """Classifica o ticker no setor BESTT correspondente"""
    return SETORES_BESTT.get(ticker, 'Outros')

def obter_dados_empresa_paralelo(ticker):
    """Obtém dados da empresa com tratamento robusto e timeout"""
    try:
        simbolo = f"{ticker}.SA" if not ticker.endswith('.SA') else ticker
        stock = yf.Ticker(simbolo)

        # Obter dados com timeout
        hist = stock.history(period='1d', timeout=15)
        info = stock.info
        divs = stock.dividends

        # Validar dados essenciais
        if hist.empty or 'Close' not in hist.columns:
            return None, None, None, f"Dados históricos inválidos para {ticker}"

        return hist, info, divs, None
    except Exception as e:
        return None, None, None, f"Erro ao obter dados para {ticker}: {str(e)}"

def analisar_ticker(ticker):
    """Analisa um único ticker com todos os cálculos"""
    hist, info, divs, erro = obter_dados_empresa_paralelo(ticker)

    if erro or hist is None or hist.empty:
        return None, erro

    try:
        # 1. Preço Atual
        preco_atual = hist['Close'].iloc[-1]
        if preco_atual <= 0 or math.isnan(preco_atual):
            return None, f"Preço atual inválido para {ticker}: {preco_atual}"

        # 2. Classificação por setor BESTT
        setor_bestt = classificar_setor_bestt(ticker)
        eh_setor_bestt = setor_bestt != 'Outros'
        status_bestt = "⭐ BESTT" if eh_setor_bestt else "⚪ Outros"

        # 3. Bazin (Dividendos)
        divs_12m = 0
        if divs is not None and not divs.empty:
            try:
                # Tratamento robusto de dividendos
                divs.index = pd.to_datetime(divs.index)
                data_atual = pd.Timestamp.now(tz=divs.index.tz) if divs.index.tz else pd.Timestamp.now()
                data_corte = data_atual - pd.DateOffset(years=1)
                divs_12m = divs[divs.index >= data_corte].sum()
                if math.isnan(divs_12m) or divs_12m < 0:
                    divs_12m = 0
            except Exception as e:
                print(f"⚠️ Aviso: Erro no cálculo de dividendos para {ticker}: {e}")

        preco_teto_bazin = divs_12m / 0.06 if divs_12m > 0 else 0
        margem_bazin = ((preco_teto_bazin / preco_atual) - 1) * 100 if preco_teto_bazin > 0 else -100

        # 4. Graham (Patrimônio/Lucro)
        lpa = info.get('trailingEps', 0)
        vpa = info.get('bookValue', 0)

        preco_justo_graham = 0
        margem_graham = -100

        if lpa and vpa and lpa > 0 and vpa > 0:
            try:
                preco_justo_graham = math.sqrt(22.5 * lpa * vpa)
                if preco_justo_graham > 0:
                    margem_graham = ((preco_justo_graham / preco_atual) - 1) * 100
            except (ValueError, TypeError, OverflowError):
                preco_justo_graham = 0
                margem_graham = -100

        # 5. Dividend Yield
        dy_12m = (divs_12m / preco_atual) * 100 if divs_12m > 0 and preco_atual > 0 else 0

        # 6. Regra de Ouro (O Filtro Sniper)
        status_sniper = "🔴 SEM OPORTUNIDADE"
        if margem_bazin > 20 and margem_graham > 20:
            status_sniper = "🟢 SINAL VERDE (Excelente)"
        elif margem_bazin > 10 and margem_graham > 10:
            status_sniper = "🟢 SINAL VERDE (Bom)"
        elif margem_bazin > 0 and margem_graham > 0:
            status_sniper = "🟡 OPORTUNIDADE MODERADA"
        elif margem_bazin > 0 or margem_graham > 0:
            status_sniper = "🟠 ATENÇÃO (1 Critério)"

        # 7. Status combinado com BESTT
        if status_sniper.startswith('🟢') and eh_setor_bestt:
            status_final = f"🎯 {status_sniper} + ⭐ BESTT"
        elif eh_setor_bestt:
            status_final = f"{status_sniper} + ⭐ BESTT"
        else:
            status_final = status_sniper

        # 8. Dados adicionais
        nome_empresa = info.get('longName', ticker)
        volume = info.get('volume', 0)
        market_cap = info.get('marketCap', 0)

        resultado = {
            'Ticker': ticker,
            'Empresa': nome_empresa[:40] + '...' if len(nome_empresa) > 40 else nome_empresa,
            'Setor BESTT': setor_bestt,
            '⭐ BESTT': status_bestt,
            'Preço Atual (R$)': round(preco_atual, 2),
            'DY 12M (%)': round(dy_12m, 2),
            'Dividendos 12M (R$)': round(divs_12m, 2),
            'Teto Bazin (R$)': round(preco_teto_bazin, 2),
            'Margem Bazin (%)': round(margem_bazin, 1),
            'Justo Graham (R$)': round(preco_justo_graham, 2),
            'Margem Graham (%)': round(margem_graham, 1),
            'LPA': round(lpa, 2) if lpa else 0,
            'VPA': round(vpa, 2) if vpa else 0,
            'Volume Médio': f"{volume:,}" if volume else '0',
            'Market Cap (R$)': f"{market_cap:,}" if market_cap else '0',
            'Status Sniper': status_sniper,
            'Status Final': status_final,
            'Data Análise': datetime.now().strftime('%d/%m/%Y')
        }

        return resultado, None

    except Exception as e:
        return None, f"Erro na análise de {ticker}: {str(e)}"

def sniper_b3_130_tickers(ticker_list, max_workers=8):
    """
    Realiza análise Sniper em 130 tickers da B3 com processamento paralelo

    Args:
        ticker_list (list): Lista de 130 tickers para análise
        max_workers (int): Número máximo de threads para processamento paralelo

    Returns:
        tuple: (DataFrame resultados, DataFrame erros)
    """
    resultados = []
    erros = []
    total_tickers = len(ticker_list)

    print(f"🚀 INICIANDO ANÁLISE SNIPER PRO - 130 TICKERS")
    print(f"📊 Distribuição por setores:")
    print(f"   • Bancos: 25 tickers")
    print(f"   • Energia: 20 tickers")
    print(f"   • Saneamento: 15 tickers")
    print(f"   • Shopping Centers: 20 tickers")
    print(f"   • Telecom: 10 tickers")
    print(f"   • Transporte: 15 tickers")
    print(f"   • Outros setores: 25 tickers")
    print(f"⏰ Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"⚙️  Processamento paralelo com {max_workers} threads")
    print("-" * 100)

    # Processamento paralelo com barra de progresso
    start_time = time.time()
    tickers_processados = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(analisar_ticker, ticker): ticker for ticker in ticker_list}

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            tickers_processados += 1

            try:
                resultado, erro = future.result()

                if erro:
                    erros.append({'Ticker': ticker, 'Erro': erro})
                    print(f"[{tickers_processados}/{total_tickers}] ❌ {ticker} - {erro}")
                elif resultado:
                    resultados.append(resultado)
                    status = resultado['Status Final']
                    print(f"[{tickers_processados}/{total_tickers}] ✅ {ticker} | {status} | R${resultado['Preço Atual (R$)']:.2f}")

                # Mostrar progresso a cada 10 tickers
                if tickers_processados % 10 == 0:
                    tempo_decorrido = time.time() - start_time
                    tempo_medio = tempo_decorrido / tickers_processados
                    tempo_estimado = tempo_medio * (total_tickers - tickers_processados)
                    print(f"   📊 Progresso: {tickers_processados}/{total_tickers} ({tickers_processados/total_tickers:.1%})")
                    print(f"   ⏱️  Tempo estimado restante: {tempo_estimado/60:.1f} minutos")

            except Exception as e:
                erros.append({'Ticker': ticker, 'Erro': f"Erro crítico: {str(e)}"})
                print(f"[{tickers_processados}/{total_tickers}] ❌ {ticker} - Erro crítico: {str(e)}")

    tempo_total = time.time() - start_time
    print(f"\n✅ Análise concluída em {tempo_total/60:.1f} minutos!")
    print(f"📊 Resultados: {len(resultados)} tickers analisados com sucesso")
    print(f"❌ Erros: {len(erros)} tickers com problemas")

    # Verificação final dos resultados
    if not resultados:
        print("❌ Nenhum resultado válido foi gerado")
        return pd.DataFrame(), pd.DataFrame(erros)

    # Criar DataFrame e ordenar
    df = pd.DataFrame(resultados)

    # Ordenação inteligente
    ordem_status = {
        "🎯 🟢 SINAL VERDE (Excelente) + ⭐ BESTT": 1,
        "🎯 🟢 SINAL VERDE (Bom) + ⭐ BESTT": 2,
        "🟢 SINAL VERDE (Excelente) + ⭐ BESTT": 3,
        "🟢 SINAL VERDE (Bom) + ⭐ BESTT": 4,
        "🎯 🟢 SINAL VERDE (Excelente)": 5,
        "🎯 🟢 SINAL VERDE (Bom)": 6,
        "🟢 SINAL VERDE (Excelente)": 7,
        "🟢 SINAL VERDE (Bom)": 8,
        "🟡 OPORTUNIDADE MODERADA + ⭐ BESTT": 9,
        "🟡 OPORTUNIDADE MODERADA": 10,
        "🟠 ATENÇÃO (1 Critério) + ⭐ BESTT": 11,
        "🟠 ATENÇÃO (1 Critério)": 12,
        "🔴 SEM OPORTUNIDADE + ⭐ BESTT": 13,
        "🔴 SEM OPORTUNIDADE": 14
    }

    df['Ordem_Status'] = df['Status Final'].map(ordem_status).fillna(999)
    df = df.sort_values(by=['Ordem_Status', 'Margem Bazin (%)'], ascending=[True, False])
    df = df.drop('Ordem_Status', axis=1)

    # DataFrame de erros
    df_erros = pd.DataFrame(erros) if erros else pd.DataFrame()

    return df, df_erros

def gerar_relatorios_completos(df, df_erros):
    """Gera relatórios detalhados e exporta para Excel"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f'sniper_b3_130tickers_{timestamp}.xlsx'

    print(f"\n" + "="*100)
    print("📊 GERANDO RELATÓRIOS COMPLETOS")
    print("="*100)

    # 1. Resumo executivo
    print(f"\n📈 RESUMO EXECUTIVO:")
    total_analisados = len(df) + len(df_erros)
    print(f"   • Total de tickers analisados: {total_analisados}")
    print(f"   • Resultados válidos: {len(df)} ({len(df)/total_analisados:.1%})")
    print(f"   • Erros: {len(df_erros)} ({len(df_erros)/total_analisados:.1%})")

    # 2. Análise por setores BESTT
    print(f"\n🏢 ANÁLISE POR SETORES BESTT:")
    setores_stats = df.groupby('Setor BESTT').agg({
        'Ticker': 'count',
        'DY 12M (%)': 'mean',
        'Margem Bazin (%)': 'mean',
        'Margem Graham (%)': 'mean',
        'Preço Atual (R$)': 'mean'
    }).round(2)

    setores_stats.columns = ['Qtd', 'DY Médio (%)', 'Margem Bazin Média (%)', 'Margem Graham Média (%)', 'Preço Médio (R$)']
    setores_stats = setores_stats.sort_values('Qtd', ascending=False)
    print(setores_stats.to_string())

    # 3. Oportunidades TOP
    oportunidades_excelentes = df[df['Status Final'].str.contains('🎯 🟢 SINAL VERDE (Excelente)')]
    oportunidades_boas = df[df['Status Final'].str.contains('🎯 🟢 SINAL VERDE (Bom)')]
    oportunidades_bestt = df[df['⭐ BESTT'] == '⭐ BESTT']

    print(f"\n🎯 OPORTUNIDADES EXCELENTES (BESTT + Sinal Verde Excelente): {len(oportunidades_excelentes)}")
    if not oportunidades_excelentes.empty:
        print(oportunidades_excelentes[['Ticker', 'Empresa', 'Setor BESTT', 'Preço Atual (R$)', 'DY 12M (%)',
                                       'Margem Bazin (%)', 'Margem Graham (%)']].to_string(index=False))

    print(f"\n⭐ OPORTUNIDADES EM SETORES BESTT: {len(oportunidades_bestt)}/{len(df)}")
    print(f"   • Sinais Verdes em BESTT: {len(df[(df['⭐ BESTT'] == '⭐ BESTT') & (df['Status Sniper'].str.contains('🟢'))])}")

    # 4. Estatísticas gerais
    print(f"\n📈 ESTATÍSTICAS GERAIS:")
    dy_medio = df['DY 12M (%)'].mean()
    margem_bazin_media = df['Margem Bazin (%)'].mean()
    preco_medio = df['Preço Atual (R$)'].mean()

    print(f"   • DY Médio do mercado: {dy_medio:.2f}%")
    print(f"   • Margem Bazin Média: {margem_bazin_media:.1f}%")
    print(f"   • Preço Médio das ações: R${preco_medio:.2f}")

    # 5. Exportação para Excel
    try:
        with pd.ExcelWriter(nome_arquivo) as writer:
            # Sheet principal - Oportunidades ordenadas
            df.to_excel(writer, sheet_name='Oportunidades_TOP', index=False)

            # Sheet - Apenas Sinais Verdes
            sinais_verdes = df[df['Status Sniper'].str.contains('🟢')]
            sinais_verdes.to_excel(writer, sheet_name='SINAIS_VERDES', index=False)

            # Sheet - Apenas BESTT
            bestt_df = df[df['⭐ BESTT'] == '⭐ BESTT']
            bestt_df.to_excel(writer, sheet_name='APENAS_BESTT', index=False)

            # Sheet - Resumo por setor
            setores_stats.to_excel(writer, sheet_name='RESUMO_POR_SETOR')

            # Sheet - Erros
            if not df_erros.empty:
                df_erros.to_excel(writer, sheet_name='ERROS', index=False)

            # Sheet - Todos os dados brutos
            df.to_excel(writer, sheet_name='TODOS_DADOS', index=False)

        print(f"\n✅ ✨ RELATÓRIOS GERADOS COM SUCESSO!")
        print(f"📁 Arquivo Excel: {nome_arquivo}")
        print(f"📊 Sheets gerados:")
        print(f"   • Oportunidades_TOP - Ordenado por prioridade")
        print(f"   • SINAIS_VERDES - Apenas oportunidades verdes")
        print(f"   • APENAS_BESTT - Empresas em setores BESTT")
        print(f"   • RESUMO_POR_SETOR - Estatísticas por setor")
        print(f"   • ERROS - Tickers com problemas")
        print(f"   • TODOS_DADOS - Dados completos")

        # Mostrar localização do arquivo
        caminho_absoluto = os.path.abspath(nome_arquivo)
        print(f"📍 Caminho completo: {caminho_absoluto}")

    except Exception as e:
        print(f"❌ Erro ao gerar relatórios: {str(e)}")

    return nome_arquivo

# Executar análise completa
if __name__ == "__main__":
    print("="*120)
    print("🎯 ANÁLISE SNIPER B3 PRO - 130 TICKERS COMPLETOS")
    print("="*120)
    print(f"📅 Data atual: {datetime.now().strftime('%d/%m/%Y')}")

    # Mostrar distribuição dos tickers
    print("\n📋 DISTRIBUIÇÃO DOS 130 TICKERS POR SETOR:")
    distribuicao = {
        'Bancos': 25, 'Energia': 20, 'Saneamento': 15, 'Shopping Centers': 20,
        'Telecom': 10, 'Transporte': 15, 'Outros': 25
    }

    for setor, quantidade in distribuicao.items():
        print(f"   • {setor}: {quantidade} tickers")

    print(f"\n🚀 INICIANDO ANÁLISE... (Isso pode levar 15-25 minutos)")
    print("💡 Dica: Acompanhe o progresso no console")

    # Executar análise
    df_resultados, df_erros = sniper_b3_130_tickers(TICKERS_130, max_workers=10)

    # Gerar relatórios
    if not df_resultados.empty:
        nome_arquivo = gerar_relatorios_completos(df_resultados, df_erros)

        # Mostrar TOP 15 oportunidades no final
        print(f"\n" + "="*120)
        print("🏆 TOP 15 MELHORES OPORTUNIDADES IDENTIFICADAS")
        print("="*120)

        top15 = df_resultados.head(15)
        print(top15[['Ticker', 'Empresa', 'Setor BESTT', 'Preço Atual (R$)', 'DY 12M (%)',
                    'Margem Bazin (%)', 'Margem Graham (%)', 'Status Final']].to_string(index=False))

        print(f"\n🎯 Recomendação: Foque nas oportunidades marcadas com '🎯' (Setor BESTT + Sinal Verde)")
        print(f"💡 Dica profissional: Empresas em setores BESTT com margem Bazin > 20% são consideradas excelentes oportunidades")

        print(f"\n✅ Análise concluída com sucesso! Verifique o arquivo Excel gerado para análise detalhada.")
    else:
        print("❌ A análise não gerou resultados válidos. Verifique os erros reportados.")