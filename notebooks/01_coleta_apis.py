# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Coleta de APIs para a landing
# MAGIC Pré-requisitos: Volume `fininsight_dev.landing.raw` (Terraform) e secrets (notebook 00).

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import hashlib
import io
import json
import os
import zipfile
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
import yfinance as yf

RAIZ = "/Volumes/fininsight_dev/landing/raw"
HOJE = date.today()
SCOPE = "fininsight"
# Primeira execucao (backfill): "2y". Execucoes diarias: "5d".
PERIODO_YF = "5d"


def segredo(chave):
    try:
        return dbutils.secrets.get(SCOPE, chave)
    except Exception:
        return None


def pasta_do_dia(fonte):
    pasta = f"{RAIZ}/{fonte}/dt={HOJE}"
    os.makedirs(pasta, exist_ok=True)
    return pasta


def salvar_ndjson(fonte, registros, nome="dados.json"):
    # um objeto JSON por linha: formato que o Auto Loader lê sem configuração extra
    with open(f"{pasta_do_dia(fonte)}/{nome}", "w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(registros)

# COMMAND ----------

def coletar_bcb():
    series = {"selic": 11, "ipca": 433, "ptax_dolar": 1}
    total = 0
    for nome, codigo in series.items():
        # a API do BCB aceita no máximo 20 registros em /ultimos/N
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados/ultimos/20?formato=json"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        registros = [{**d, "serie": nome} for d in r.json()]
        total += salvar_ndjson(f"bcb/{nome}", registros)
    return f"{total} registros"


def coletar_yfinance():
    tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP", "AAPL", "MSFT"]
    frames = []
    for t in tickers:
        d = yf.download(t, period=PERIODO_YF, interval="1d", auto_adjust=False, progress=False)
        if d.empty:
            continue
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        d = d.reset_index()
        d.columns.name = None
        d["ticker"] = t
        frames.append(d)
    if not frames:
        raise RuntimeError("yfinance nao retornou dados")
    df = pd.concat(frames, ignore_index=True)
    df.columns = [c.replace(" ", "_") for c in df.columns]   # "Adj Close" -> "Adj_Close"
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    # o Spark nao le timestamp em nanossegundos: gravar em milissegundos
    df.to_parquet(
        f"{pasta_do_dia('yfinance')}/precos.parquet",
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
    )
    return f"{len(df)} linhas"


def coletar_b3():
    # arquivo diario COTAHIST da B3: tenta os ultimos dias uteis ate achar um publicado
    for i in range(10):
        dia = HOJE - timedelta(days=i)
        if dia.weekday() >= 5:
            continue
        url = f"https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{dia:%d%m%Y}.ZIP"
        r = requests.get(url, timeout=120)
        if r.status_code != 200:
            continue
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            texto = z.read(z.namelist()[0]).decode("latin-1")
        linhas = []
        for ln in texto.splitlines():
            # registro tipo 01 (cotacao) e mercados 010 (vista) / 020 (fracionario)
            if ln[0:2] != "01" or ln[24:27] not in ("010", "020"):
                continue
            linhas.append({
                "codigo": ln[12:24].strip(),
                "data_pregao": f"{ln[2:6]}-{ln[6:8]}-{ln[8:10]}",
                "preco_abertura": int(ln[56:69]) / 100,
                "preco_maximo": int(ln[69:82]) / 100,
                "preco_minimo": int(ln[82:95]) / 100,
                "preco_fechamento": int(ln[108:121]) / 100,
                "volume": int(ln[170:188]) / 100,
            })
        if not linhas:
            continue
        pd.DataFrame(linhas).to_csv(f"{pasta_do_dia('b3')}/cotacoes.csv", index=False)
        return f"{len(linhas)} linhas ({dia})"
    raise RuntimeError("nenhum arquivo COTAHIST diario encontrado nos ultimos 10 dias")


def coletar_alphavantage():
    chave = segredo("alphavantage-api-key")
    if not chave:
        return "ignorado (sem chave alphavantage-api-key)"
    r = requests.get(
        "https://www.alphavantage.co/query",
        params={"function": "FX_DAILY", "from_symbol": "USD", "to_symbol": "BRL",
                "outputsize": "compact", "apikey": chave},
        timeout=30,
    )
    r.raise_for_status()
    corpo = r.json()
    serie = corpo.get("Time Series FX (Daily)")
    if not serie:
        raise RuntimeError(f"resposta inesperada do Alpha Vantage: {str(corpo)[:300]}")
    registros = [{
        "par": "USDBRL", "data": dia,
        "open": float(v["1. open"]), "high": float(v["2. high"]),
        "low": float(v["3. low"]), "close": float(v["4. close"]),
    } for dia, v in serie.items()]
    return f"{salvar_ndjson('alphavantage', registros)} registros"


def coletar_fred():
    chave = segredo("fred-api-key")
    if not chave:
        return "ignorado (sem chave fred-api-key)"
    registros = []
    for serie in ["FEDFUNDS", "CPIAUCSL", "DGS10"]:
        r = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={"series_id": serie, "api_key": chave, "file_type": "json",
                    "sort_order": "desc", "limit": 24},
            timeout=30,
        )
        r.raise_for_status()
        registros += [{"serie": serie, "data": o["date"], "valor": o["value"]}
                      for o in r.json()["observations"]]
    return f"{salvar_ndjson('fred', registros)} registros"


def coletar_coingecko():
    headers = {}
    chave = segredo("coingecko-key")
    if chave:
        headers["x-cg-demo-api-key"] = chave
    r = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "ids": "bitcoin,ethereum,solana"},
        headers=headers, timeout=30,
    )
    r.raise_for_status()
    agora = datetime.now(timezone.utc).isoformat()
    registros = [{
        "id": c["id"], "simbolo": c["symbol"], "preco_usd": c["current_price"],
        "market_cap": c["market_cap"], "volume_24h": c["total_volume"], "coletado_em": agora,
    } for c in r.json()]
    return f"{salvar_ndjson('coingecko', registros)} registros"


def coletar_noticias():
    chave = segredo("newsapi-key")
    if not chave:
        return "ignorado (sem chave newsapi-key)"
    r = requests.get(
        "https://newsapi.org/v2/everything",
        params={"q": "Petrobras OR Vale OR Itau OR Ibovespa OR Selic", "language": "pt",
                "sortBy": "publishedAt", "pageSize": 50, "apiKey": chave},
        timeout=30,
    )
    r.raise_for_status()
    mapa = {"Petrobras": "PETR4.SA", "Vale": "VALE3.SA", "Ita": "ITUB4.SA"}
    registros = []
    for a in r.json().get("articles", []):
        texto = " ".join(filter(None, [a.get("title"), a.get("description"), a.get("content")]))
        registros.append({
            "noticia_id": hashlib.md5(a["url"].encode()).hexdigest(),
            "titulo": a.get("title"),
            "texto_noticia": texto,
            "fonte": (a.get("source") or {}).get("name"),
            "data_publicacao": a.get("publishedAt"),
            "url": a["url"],
            "ticker_relacionado": next((t for k, t in mapa.items() if k in texto), None),
        })
    return f"{salvar_ndjson('noticias', registros)} registros"

# COMMAND ----------

tarefas = {
    "bcb": coletar_bcb,
    "yfinance": coletar_yfinance,
    "b3": coletar_b3,
    "alphavantage": coletar_alphavantage,
    "fred": coletar_fred,
    "coingecko": coletar_coingecko,
    "noticias": coletar_noticias,
}

falhas = {}
for nome, fn in tarefas.items():
    try:
        print(f"[OK]    {nome}: {fn()}")
    except Exception as e:
        falhas[nome] = repr(e)[:300]
        print(f"[FALHA] {nome}: {falhas[nome]}")

if falhas:
    raise RuntimeError(f"fontes com falha: {list(falhas)}")
