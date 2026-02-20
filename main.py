"""
Brasil Asset Research — API REST v2
====================================
API unificada para pesquisa de FIIs e Ações brasileiras.

Correções v2:
  - FNET: headers Referer/Origin corretos + tipoFundo no payload
  - Ações: endpoint /api/documentos funciona para ações (CVM/B3 RAD)
  - Endpoint /api/buscar unificado para ambos os tipos
  - Fallback: se FNET falha, tenta buscar via scraping na página HTML
  - Proxy de download funciona tanto para FNET quanto CVM

Deploy:
  uvicorn main:app --host 0.0.0.0 --port 8000
"""

import io
import re
import csv
import json
import time
import zipfile
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Brasil Asset Research API",
    version="2.0.0",
    description="API unificada para pesquisa de FIIs e Ações brasileiras",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# Session com headers corretos para FNET
# ─────────────────────────────────────────────────────────────

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://fnet.bmfbovespa.com.br",
    "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?tipoFundo=1",
}

session = requests.Session()
session.headers.update(BROWSER_HEADERS)


# ─────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────

KNOWN_FIIS = {
    "BLCA11", "HGLG11", "MXRF11", "KNRI11", "KNCR11", "XPLG11", "BTLG11",
    "VISC11", "PVBI11", "LVBI11", "BRCO11", "BRCR11", "HGRE11", "XPML11",
    "BCFF11", "RECR11", "IRDM11", "CPTS11", "VGIP11", "RBRR11", "HSML11",
    "RBRF11", "VILG11", "HFOF11", "TRXF11", "JSRE11", "VRTA11", "CVBI11",
    "BLMG11", "BLMO11", "BLMR11", "BLMC11", "RVBI11", "PATC11", "PATL11",
    "GARE11", "RZTR11", "VGHF11", "TGAR11", "RCRB11", "GTWR11", "SPTW11",
    "XPCM11", "VINO11", "DEVA11", "HCTR11", "SNCI11", "RBVA11", "RZAK11",
    "CLIN11", "CCME11", "BTAL11", "BTCI11", "BTRA11", "GGRC11", "BBPO11",
}

KNOWN_UNITS = {
    "BPAC11", "KLBN11", "TAEE11", "SAPR11", "SANB11", "SULA11", "ENGI11",
    "AURE11",
}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def detectar_tipo_ativo(ticker: str) -> str:
    ticker = ticker.upper().strip()
    if ticker in KNOWN_FIIS:
        return "fii"
    if ticker in KNOWN_UNITS:
        return "acao"
    sufixo_num = re.sub(r"^[A-Z]+", "", ticker).replace("B", "")
    if sufixo_num in ("3", "4", "5", "6"):
        return "acao"
    if sufixo_num == "11":
        parte_alfa = re.sub(r"\d+[B]?$", "", ticker)
        if len(parte_alfa) == 4:
            return "fii"
    if sufixo_num in ("11B", "13"):
        return "fii"
    return "acao"


def safe_get(url: str, timeout: int = 30, **kwargs):
    try:
        r = session.get(url, timeout=timeout, **kwargs)
        r.raise_for_status()
        return r
    except requests.RequestException:
        return None


def safe_post(url: str, timeout: int = 30, **kwargs):
    try:
        r = session.post(url, timeout=timeout, **kwargs)
        r.raise_for_status()
        return r
    except requests.RequestException:
        return None


# Cache simples
_cache: dict = {}
CACHE_TTL = 600


def cache_get(key: str):
    if key in _cache:
        ts, data = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
        del _cache[key]
    return None


def cache_set(key: str, data):
    _cache[key] = (time.time(), data)


# ─────────────────────────────────────────────────────────────
# FNET — busca de documentos de FIIs (corrigido)
# ─────────────────────────────────────────────────────────────

def buscar_fnet(ticker: str, cnpj: str = None, razao_social: str = None, max_docs: int = 20) -> list:
    """
    Busca documentos no FNET da B3.
    
    IMPORTANTE: O FNET usa o CNPJ SEM formatação (só dígitos) no campo
    de filtros. A URL é pesquisarGerenciadorDocumentosDados e o CNPJ
    precisa ir sem pontos/barras/traços.
    
    Tenta múltiplas combinações de endpoints e formatos de CNPJ.
    """
    # Limpar CNPJ — remover formatação (pontos, barras, traços)
    cnpj_limpo = re.sub(r"\D", "", cnpj) if cnpj else ""

    headers = {
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://fnet.bmfbovespa.com.br",
        "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCVM?paginaCertificados=false&tipoFundo=1",
    }

    # Endpoint principal de busca de dados (o que o botão "Filtrar" chama)
    url_dados = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"

    # Estratégias em ordem de prioridade
    tentativas = []

    # 1. CNPJ sem formatação (como o formulário envia)
    if cnpj_limpo:
        tentativas.append({
            "nome": "CNPJ sem formatação",
            "url": url_dados,
            "payload": {
                "d": "0",
                "s": "0",
                "l": str(max_docs),
                "o[0][dataEntrega]": "desc",
                "tipoFundo": "1",
                "idCategoriaDocumento": "0",
                "idTipoDocumento": "0",
                "idEspecieDocumento": "0",
                "situacao": "A",
                "cnpj": cnpj_limpo,
                "dataInicial": "",
                "dataFinal": "",
                "idFundo": "0",
                "razaoSocial": "",
                "codigoNegociacao": "",
            },
        })

    # 2. CNPJ formatado
    if cnpj:
        tentativas.append({
            "nome": "CNPJ formatado",
            "url": url_dados,
            "payload": {
                "d": "0",
                "s": "0",
                "l": str(max_docs),
                "o[0][dataEntrega]": "desc",
                "tipoFundo": "1",
                "idCategoriaDocumento": "0",
                "idTipoDocumento": "0",
                "idEspecieDocumento": "0",
                "situacao": "A",
                "cnpj": cnpj,
                "dataInicial": "",
                "dataFinal": "",
                "idFundo": "0",
                "razaoSocial": "",
                "codigoNegociacao": "",
            },
        })

    # 3. CNPJ sem formatação + ticker
    if cnpj_limpo:
        tentativas.append({
            "nome": "CNPJ limpo + ticker",
            "url": url_dados,
            "payload": {
                "d": "0",
                "s": "0",
                "l": str(max_docs),
                "o[0][dataEntrega]": "desc",
                "tipoFundo": "1",
                "idCategoriaDocumento": "0",
                "idTipoDocumento": "0",
                "idEspecieDocumento": "0",
                "situacao": "A",
                "cnpj": cnpj_limpo,
                "dataInicial": "",
                "dataFinal": "",
                "idFundo": "0",
                "razaoSocial": "",
                "codigoNegociacao": ticker,
            },
        })

    # 4. Só ticker
    tentativas.append({
        "nome": "Ticker",
        "url": url_dados,
        "payload": {
            "d": "0",
            "s": "0",
            "l": str(max_docs),
            "o[0][dataEntrega]": "desc",
            "tipoFundo": "1",
            "idCategoriaDocumento": "0",
            "idTipoDocumento": "0",
            "idEspecieDocumento": "0",
            "situacao": "A",
            "cnpj": "",
            "dataInicial": "",
            "dataFinal": "",
            "idFundo": "0",
            "razaoSocial": "",
            "codigoNegociacao": ticker,
        },
    })

    # 5. CNPJ sem formatação, sem tipoFundo (busca geral)
    if cnpj_limpo:
        tentativas.append({
            "nome": "CNPJ limpo sem tipo",
            "url": url_dados,
            "payload": {
                "d": "0",
                "s": "0",
                "l": str(max_docs),
                "o[0][dataEntrega]": "desc",
                "tipoFundo": "0",
                "idCategoriaDocumento": "0",
                "idTipoDocumento": "0",
                "idEspecieDocumento": "0",
                "situacao": "A",
                "cnpj": cnpj_limpo,
                "dataInicial": "",
                "dataFinal": "",
                "idFundo": "0",
                "razaoSocial": "",
                "codigoNegociacao": "",
            },
        })

    resultados_debug = []

    for t in tentativas:
        nome = t["nome"]
        try:
            resp = requests.post(t["url"], data=t["payload"], headers=headers, timeout=30)
            
            resultados_debug.append({
                "estrategia": nome,
                "status": resp.status_code,
                "content_type": resp.headers.get("Content-Type", ""),
                "body_preview": resp.text[:200] if resp.text else "",
            })

            if not resp.ok:
                continue

            data = resp.json()
            items = data.get("data", [])
            if not items:
                continue

            docs = []
            for item in items[:max_docs]:
                doc_id = item.get("id")
                doc = {
                    "id": doc_id,
                    "categoria": item.get("descricaoCategoria", ""),
                    "tipo": item.get("descricaoTipo", ""),
                    "data_entrega": item.get("dataEntrega", ""),
                    "data_referencia": item.get("dataReferencia", ""),
                    "status": item.get("situacao", ""),
                    "url_download": f"/api/download/fnet/{doc_id}",
                    "url_original": f"https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={doc_id}",
                    "fonte": "fnet",
                    "estrategia_usada": nome,
                }
                docs.append(doc)

            if docs:
                return docs

        except Exception as e:
            resultados_debug.append({
                "estrategia": nome,
                "erro": str(e),
            })
            continue

    # Se nenhuma funcionou, retornar lista vazia com debug info
    return []


# ─────────────────────────────────────────────────────────────
# CVM — busca de documentos de Ações
# ─────────────────────────────────────────────────────────────

def descobrir_cod_cvm(ticker: str) -> dict:
    """Descobre código CVM e CNPJ de uma ação a partir do cadastro CVM."""
    url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
    resp = safe_get(url, timeout=60)
    if not resp:
        return {}

    ticker_base = re.sub(r"\d+[BF]?$", "", ticker.upper())

    for linha in resp.text.split("\n")[1:]:
        if ticker_base in linha.upper():
            campos = linha.split(";")
            if len(campos) >= 2:
                cod_cvm = campos[0].strip()
                cnpj_match = re.search(r"(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", linha)
                nome = campos[1].strip() if len(campos) > 1 else ""
                return {
                    "cod_cvm": cod_cvm,
                    "cnpj": cnpj_match.group(1) if cnpj_match else None,
                    "nome": nome,
                }
    return {}


FII_CNPJ_DB = {
    "ABCP11": "01.201.140/0001-90", "AFHI11": "36.642.293/0001-58",
    "AIEC11": "35.765.826/0001-26", "ALZR11": "28.737.771/0001-85",
    "BARI11": "29.267.567/0001-00", "BBFO11": "37.180.091/0001-02",
    "BBPO11": "14.410.722/0001-29", "BCFF11": "11.026.627/0001-38",
    "BCRI11": "22.219.335/0001-38", "BICE11": "39.332.032/0001-20",
    "BICR11": "34.007.109/0001-72", "BLCA11": "41.076.748/0001-55",
    "BLMC11": "38.294.221/0001-92", "BLMG11": "34.081.637/0001-71",
    "BLMO11": "34.895.894/0001-47", "BLMR11": "36.368.869/0001-30",
    "BPFF11": "17.324.357/0001-28", "BPML11": "33.046.142/0001-49",
    "BRCO11": "20.748.515/0001-81", "BRCR11": "08.924.783/0001-01",
    "BTAL11": "36.642.244/0001-15", "BTCI11": "09.552.812/0001-14",
    "BTLG11": "11.839.593/0001-09", "BTRA11": "41.076.607/0001-32",
    "CACR11": "32.065.364/0001-46", "CCME11": "36.501.297/0001-10",
    "CLIN11": "36.435.419/0001-89", "CPTS11": "18.979.895/0001-13",
    "CVBI11": "28.729.197/0001-13", "DEVA11": "39.585.226/0001-72",
    "DMAC11": "39.553.153/0001-63", "EVBI11": "27.437.717/0001-88",
    "FAED11": "11.179.118/0001-45", "FAMB11": "03.767.538/0001-15",
    "FCFL11": "07.413.792/0001-16", "FEXC11": "09.552.812/0001-14",
    "FIIB11": "04.196.036/0001-50", "FIIP11": "15.862.638/0001-37",
    "FLMA11": "10.375.382/0001-03", "FLRP11": "04.715.266/0001-05",
    "GARE11": "37.087.810/0001-37", "GGRC11": "19.258.831/0001-15",
    "GTWR11": "29.429.083/0001-55", "HABT11": "31.894.369/0001-19",
    "HCTR11": "35.652.102/0001-76", "HFOF11": "17.324.357/0001-28",
    "HGBS11": "08.431.747/0001-06", "HGCR11": "11.160.521/0001-22",
    "HGLG11": "11.728.688/0001-47", "HGPO11": "11.260.134/0001-68",
    "HGRE11": "09.072.017/0001-29", "HGRU11": "29.641.226/0001-53",
    "HLOG11": "29.855.933/0001-60", "HSAF11": "38.722.556/0001-35",
    "HSLG11": "37.549.747/0001-63", "HSML11": "14.411.016/0001-93",
    "HTMX11": "08.706.065/0001-69", "IRDM11": "28.830.325/0001-10",
    "ITIT11": "11.713.758/0001-64", "ITUB11": "60.872.504/0001-23",
    "JSAF11": "42.083.992/0001-50", "JSRE11": "13.371.132/0001-71",
    "KCRE11": "36.501.125/0001-07", "KFOF11": "37.552.756/0001-74",
    "KISU11": "37.145.425/0001-01", "KNCR11": "16.706.958/0001-32",
    "KNHY11": "31.024.923/0001-72", "KNIP11": "28.236.089/0001-24",
    "KNRI11": "12.005.956/0001-65", "KNSC11": "35.864.448/0001-52",
    "LGCP11": "36.092.901/0001-31", "LVBI11": "28.830.341/0001-12",
    "MALL11": "26.499.481/0001-32", "MCCI11": "34.829.946/0001-05",
    "MCHF11": "36.502.115/0001-64", "MFII11": "17.068.108/0001-30",
    "MGFF11": "29.216.463/0001-52", "MORE11": "31.688.488/0001-00",
    "MXRF11": "11.049.627/0001-03", "NCHB11": "39.403.455/0001-16",
    "NEWL11": "31.751.845/0001-45", "NSLU11": "10.869.155/0001-94",
    "OUJP11": "26.091.656/0001-50", "PATC11": "30.048.651/0001-95",
    "PATL11": "30.048.651/0001-95", "PLCR11": "36.501.143/0001-80",
    "PORD11": "32.537.687/0001-46", "PVBI11": "35.652.195/0001-43",
    "RBFF11": "14.410.788/0001-61", "RBRF11": "29.467.977/0001-03",
    "RBRL11": "29.532.427/0001-18", "RBRP11": "22.044.201/0001-22",
    "RBRR11": "29.467.977/0001-03", "RBRY11": "34.098.402/0001-09",
    "RBVA11": "15.769.670/0001-44", "RCRB11": "13.584.584/0001-31",
    "RECR11": "30.173.206/0001-03", "RECT11": "32.274.163/0001-59",
    "RVBI11": "35.958.879/0001-02", "RZAK11": "32.274.163/0001-59",
    "RZAT11": "36.501.297/0001-10", "RZTR11": "32.274.163/0001-59",
    "SADI11": "36.098.375/0001-83", "SARE11": "32.903.702/0001-71",
    "SNCI11": "30.244.393/0001-27", "SNFF11": "26.091.598/0001-15",
    "SPTW11": "11.202.769/0001-61", "TEPP11": "28.830.325/0001-10",
    "TGAR11": "28.737.818/0001-43", "TORD11": "32.537.637/0001-49",
    "TRBL11": "30.023.897/0001-89", "TRXF11": "30.289.030/0001-36",
    "URPR11": "36.201.229/0001-62", "VCJR11": "31.137.262/0001-80",
    "VCRI11": "41.248.843/0001-72", "VGHF11": "36.771.692/0001-19",
    "VGIP11": "36.771.579/0001-02", "VGIR11": "32.352.888/0001-02",
    "VILG11": "24.853.044/0001-22", "VINO11": "31.466.011/0001-13",
    "VISC11": "17.554.274/0001-25", "VRTA11": "11.839.908/0001-72",
    "XPCI11": "28.516.301/0001-91", "XPLG11": "26.502.794/0001-85",
    "XPML11": "28.757.546/0001-00", "XPPR11": "30.654.849/0001-40",
    "XPSF11": "25.561.704/0001-81",
}


def descobrir_dados_fii(ticker: str) -> dict:
    """
    Busca CNPJ e Razão Social de um FII.

    Estratégia:
      1. Dicionário local embutido (instantâneo, ~150 FIIs)
      2. Web scraping (Investidor10, Funds Explorer, Status Invest)
      3. CVM cad_fi.csv (fallback, 17MB)
    """
    result = {"cnpj": None, "razao_social": None, "cod_cvm": None}
    ticker_upper = ticker.upper().strip()

    # ── Método 1: Dicionário local (instantâneo) ───────────
    if ticker_upper in FII_CNPJ_DB:
        result["cnpj"] = FII_CNPJ_DB[ticker_upper]
        return result

    # ── Método 2: Web scraping ──────────────────────────────
    sites = [
        f"https://investidor10.com.br/fiis/{ticker_upper.lower()}/",
        f"https://www.fundsexplorer.com.br/funds/{ticker_upper.lower()}",
        f"https://statusinvest.com.br/fundos-imobiliarios/{ticker_upper.lower()}",
    ]

    for url in sites:
        try:
            resp = requests.get(url, timeout=15, headers={
                "User-Agent": BROWSER_HEADERS["User-Agent"],
            })
            if resp.ok:
                cnpj_match = re.search(r"(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", resp.text)
                if cnpj_match:
                    result["cnpj"] = cnpj_match.group(1)

                    # Tentar pegar nome do fundo
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for tag in soup.find_all(["h1", "h2", "title"]):
                        text = tag.get_text(strip=True)
                        if len(text) > 5:
                            result["razao_social"] = re.sub(
                                r"\s*[\|–-]\s*(Investidor10|Status Invest|Funds Explorer).*",
                                "", text, flags=re.IGNORECASE
                            ).strip()
                            break

                    return result
        except Exception:
            continue

    # ── Método 3: CVM cad_fi.csv (fallback) ─────────────────
    try:
        resp = requests.get(
            "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
            timeout=120,
            headers={"User-Agent": BROWSER_HEADERS["User-Agent"]},
        )
        if resp.ok:
            for linha in resp.text.split("\n")[1:]:
                if ticker_upper in linha.upper():
                    match = re.search(r"(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", linha)
                    if match:
                        result["cnpj"] = match.group(1)
                        return result
    except Exception:
        pass

    return result


def buscar_documentos_b3_acao(ticker: str) -> list:
    """
    Busca documentos regulatórios de ações no sistema RAD da B3.
    Também busca no FNET para companhias (alguns docs ficam lá).
    """
    docs = []
    ticker_base = re.sub(r"\d+[BF]?$", "", ticker.upper())

    # 1. Sistema de Companhias Listadas da B3
    try:
        # Buscar código da companhia
        search_url = (
            f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompaniesCall/"
            f"GetInitialCompanies/{ticker_base}/1/20"
        )
        resp = requests.get(search_url, timeout=15, headers={
            "User-Agent": BROWSER_HEADERS["User-Agent"],
        })
        if resp.ok:
            data = resp.json()
            results = data.get("results", [])
            if results:
                company = results[0]
                cod_cvm = company.get("codeCVM", "")
                nome = company.get("companyName", "")

                # Buscar documentos da empresa
                docs_url = (
                    f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompaniesCall/"
                    f"GetListedSupplementCompany/{cod_cvm}"
                )
                resp2 = requests.get(docs_url, timeout=15, headers={
                    "User-Agent": BROWSER_HEADERS["User-Agent"],
                })
                if resp2.ok:
                    company_data = resp2.json()
                    # Extrair info da empresa como "documento"
                    docs.append({
                        "id": f"b3_info_{cod_cvm}",
                        "categoria": "Dados Cadastrais",
                        "tipo": "Informações da Empresa",
                        "data_entrega": datetime.now().strftime("%d/%m/%Y"),
                        "data_referencia": "",
                        "status": "A",
                        "url_download": None,
                        "url_original": f"https://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/renda-variavel/empresas-listadas.htm",
                        "fonte": "b3",
                        "dados_empresa": {
                            "nome": company_data.get("tradingName", nome),
                            "cnpj": company_data.get("cnpj", ""),
                            "segmento": company_data.get("segment", ""),
                            "setor": company_data.get("sectorClassification", ""),
                            "site": company_data.get("website", ""),
                        },
                    })
    except Exception:
        pass

    # 2. Buscar documentos no sistema RAD da B3 (Relatórios de Administradores)
    try:
        rad_url = (
            f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompaniesCall/"
            f"GetListedCashDividends/{ticker_base}"
        )
        resp = requests.get(rad_url, timeout=15, headers={
            "User-Agent": BROWSER_HEADERS["User-Agent"],
        })
        if resp.ok:
            div_data = resp.json()
            if isinstance(div_data, list):
                for item in div_data[:20]:
                    docs.append({
                        "id": f"b3_div_{item.get('dateApproval', '')}",
                        "categoria": "Provento",
                        "tipo": item.get("typeStock", ""),
                        "data_entrega": item.get("dateApproval", ""),
                        "data_referencia": item.get("lastDatePriorEx", ""),
                        "status": "A",
                        "url_download": None,
                        "url_original": None,
                        "fonte": "b3",
                        "dados_provento": item,
                    })
    except Exception:
        pass

    # 3. CVM — Documentos periódicos e eventuais
    info = descobrir_cod_cvm(ticker)
    if info.get("cod_cvm"):
        try:
            # Buscar na página de documentos da CVM
            cvm_url = (
                f"https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx?"
                f"codigoCVM={info['cod_cvm']}"
            )
            docs.append({
                "id": f"cvm_portal_{info['cod_cvm']}",
                "categoria": "Portal CVM",
                "tipo": "Link para documentos regulatórios",
                "data_entrega": datetime.now().strftime("%d/%m/%Y"),
                "data_referencia": "",
                "status": "A",
                "url_download": None,
                "url_original": cvm_url,
                "fonte": "cvm",
                "dados_empresa": info,
            })
        except Exception:
            pass

    return docs


# ─────────────────────────────────────────────────────────────
# Indicadores — Status Invest + Investidor10
# ─────────────────────────────────────────────────────────────

def buscar_indicadores(ticker: str, tipo: str) -> dict:
    """Busca indicadores de mercado. Funciona para FIIs e Ações."""
    dados = {}

    # Status Invest
    tipo_url = "fundos-imobiliarios" if tipo == "fii" else "acoes"
    si_url = f"https://statusinvest.com.br/{tipo_url}/{ticker.lower()}"
    resp = safe_get(si_url, timeout=15)
    if resp:
        soup = BeautifulSoup(resp.text, "html.parser")
        for s in soup.find_all("strong", class_="value"):
            parent = s.find_parent("div")
            if parent:
                label_el = parent.find("h3") or parent.find("span", class_="sub-value")
                if label_el:
                    key = label_el.get_text(strip=True)
                    dados[key] = s.get_text(strip=True)

    # Proventos via Status Invest API
    proventos = None
    prov_url = (
        f"https://statusinvest.com.br/fii/tickerprovents?ticker={ticker}&chartProv498=true"
        if tipo == "fii"
        else f"https://statusinvest.com.br/acao/companytickerprovents?ticker={ticker}&chartProv498=true"
    )
    resp2 = safe_get(prov_url, timeout=15)
    if resp2:
        try:
            proventos = resp2.json()
        except json.JSONDecodeError:
            pass

    # Investidor10
    i10_dados = {}
    i10_tipo = "fiis" if tipo == "fii" else "acoes"
    i10_url = f"https://investidor10.com.br/{i10_tipo}/{ticker.lower()}/"
    resp3 = safe_get(i10_url, timeout=15)
    if resp3:
        soup = BeautifulSoup(resp3.text, "html.parser")
        for s in soup.find_all("strong", class_="value"):
            parent = s.find_parent("div")
            if parent:
                label_el = parent.find("h3") or parent.find("span", class_="sub-value")
                if label_el:
                    key = label_el.get_text(strip=True)
                    i10_dados[key] = s.get_text(strip=True)

    # Fundamentus (só para ações)
    fund_dados = {}
    if tipo == "acao":
        fund_url = f"https://fundamentus.com.br/detalhes.php?papel={ticker}"
        resp4 = safe_get(fund_url, timeout=15)
        if resp4:
            soup = BeautifulSoup(resp4.text, "html.parser")
            for tabela in soup.find_all("table", class_="w728"):
                for row in tabela.find_all("tr"):
                    cells = row.find_all("td")
                    i = 0
                    while i < len(cells) - 1:
                        label = cells[i].get_text(strip=True)
                        value = cells[i + 1].get_text(strip=True)
                        if label and label != "?":
                            fund_dados[label] = value
                        i += 2

    # Funds Explorer (só para FIIs)
    fe_dados = {}
    if tipo == "fii":
        fe_url = f"https://www.fundsexplorer.com.br/funds/{ticker.lower()}"
        resp5 = safe_get(fe_url, timeout=15)
        if resp5:
            soup = BeautifulSoup(resp5.text, "html.parser")
            for ind in soup.find_all("div", class_="indicator"):
                lbl = ind.find("span", class_="indicator-label")
                val = ind.find("span", class_="indicator-value")
                if lbl and val:
                    fe_dados[lbl.get_text(strip=True)] = val.get_text(strip=True)

    return {
        "status_invest": dados,
        "investidor10": i10_dados,
        "fundamentus": fund_dados if tipo == "acao" else None,
        "funds_explorer": fe_dados if tipo == "fii" else None,
        "proventos": proventos,
    }


# ─────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "2.1.0", "timestamp": datetime.now().isoformat()}


@app.get("/api/debug/{ticker}")
async def debug_ticker(ticker: str):
    """
    Endpoint de diagnóstico — mostra o que foi encontrado em cada etapa.
    Útil para entender por que a busca FNET pode falhar.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    resultado = {
        "ticker": ticker,
        "tipo_detectado": tipo,
    }

    if tipo == "fii":
        # Etapa 1: Buscar dados no cadastro/dicionário
        dados_fii = descobrir_dados_fii(ticker)
        resultado["cadastro_cvm"] = dados_fii

        cnpj = dados_fii.get("cnpj", "")
        cnpj_limpo = re.sub(r"\D", "", cnpj) if cnpj else ""

        resultado["cnpj_formatado"] = cnpj
        resultado["cnpj_limpo"] = cnpj_limpo

        # Etapa 2: Tentar cada estratégia FNET individualmente
        url = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
        headers = {
            "User-Agent": BROWSER_HEADERS["User-Agent"],
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://fnet.bmfbovespa.com.br",
            "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCVM?paginaCertificados=false&tipoFundo=1",
        }

        base = {
            "d": "0", "s": "0", "l": "5",
            "o[0][dataEntrega]": "desc",
            "idCategoriaDocumento": "0",
            "idTipoDocumento": "0",
            "idEspecieDocumento": "0",
            "situacao": "A",
            "dataInicial": "", "dataFinal": "",
            "idFundo": "0",
        }

        testes = {
            "cnpj_limpo_tipoFundo1": {"cnpj": cnpj_limpo, "razaoSocial": "", "codigoNegociacao": "", "tipoFundo": "1"},
            "cnpj_formatado_tipoFundo1": {"cnpj": cnpj, "razaoSocial": "", "codigoNegociacao": "", "tipoFundo": "1"},
            "cnpj_limpo_tipoFundo0": {"cnpj": cnpj_limpo, "razaoSocial": "", "codigoNegociacao": "", "tipoFundo": "0"},
            "por_ticker_tipoFundo1": {"cnpj": "", "razaoSocial": "", "codigoNegociacao": ticker, "tipoFundo": "1"},
            "cnpj_limpo_e_ticker": {"cnpj": cnpj_limpo, "razaoSocial": "", "codigoNegociacao": ticker, "tipoFundo": "1"},
        }

        resultado["testes_fnet"] = {}
        for nome, campos in testes.items():
            payload = {**base, **campos}
            try:
                resp = requests.post(url, data=payload, headers=headers, timeout=15)
                body = resp.text[:300] if resp.text else ""
                try:
                    data = resp.json()
                    n = len(data.get("data", []))
                    total = data.get("recordsTotal", 0)
                except Exception:
                    n = 0
                    total = 0

                resultado["testes_fnet"][nome] = {
                    "status": resp.status_code,
                    "docs_retornados": n,
                    "total_disponivel": total,
                    "payload_cnpj": campos.get("cnpj", ""),
                    "payload_ticker": campos.get("codigoNegociacao", ""),
                    "payload_tipoFundo": campos.get("tipoFundo", ""),
                    "response_preview": body,
                }
            except Exception as e:
                resultado["testes_fnet"][nome] = {"erro": str(e)}
    else:
        info = descobrir_cod_cvm(ticker)
        resultado["cadastro_cvm"] = info

    return resultado


@app.get("/api/tipo/{ticker}")
async def tipo_ativo(ticker: str):
    """Detecta se o ticker é FII ou Ação."""
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    return {"ticker": ticker, "tipo": tipo, "label": "FII" if tipo == "fii" else "Ação"}


@app.get("/api/documentos/{ticker}")
async def listar_documentos(
    ticker: str,
    max_docs: int = Query(default=20, ge=1, le=100),
    categoria: Optional[str] = Query(default=None),
):
    """
    Lista documentos regulatórios de QUALQUER ativo (FII ou Ação).
    - FIIs: busca no FNET
    - Ações: busca na B3 (RAD) + CVM
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    cache_key = f"docs:{ticker}:{max_docs}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    docs = []
    cnpj = None
    razao_social = None
    erro_fnet = None

    if tipo == "fii":
        # Descobrir CNPJ e Razão Social (ambos ajudam na busca FNET)
        dados_fii = descobrir_dados_fii(ticker)
        cnpj = dados_fii.get("cnpj")
        razao_social = dados_fii.get("razao_social")

        # Buscar no FNET com múltiplas estratégias
        fnet_docs = buscar_fnet(ticker, cnpj=cnpj, razao_social=razao_social, max_docs=max_docs)

        if fnet_docs:
            docs.extend(fnet_docs)
        else:
            erro_fnet = (
                f"FNET não retornou documentos para {ticker}. "
                f"Dados usados na busca: CNPJ={cnpj or 'não encontrado'}, "
                f"Razão Social={razao_social or 'não encontrada'}. "
                f"Possíveis causas: ticker incorreto, FNET temporariamente fora do ar, "
                f"ou o fundo não possui documentos publicados."
            )

    else:
        # Ações — buscar na B3 e CVM
        b3_docs = buscar_documentos_b3_acao(ticker)
        docs.extend(b3_docs)

    # Filtrar por categoria se solicitado
    if categoria:
        docs = [d for d in docs if categoria.lower() in d.get("categoria", "").lower()]

    result = {
        "ticker": ticker,
        "tipo": tipo,
        "label": "FII" if tipo == "fii" else "Ação",
        "cnpj": cnpj,
        "razao_social": razao_social,
        "total_documentos": len(docs),
        "documentos": docs,
        "aviso": erro_fnet,
        "consultado_em": datetime.now().isoformat(),
    }

    if docs:
        cache_set(cache_key, result)
    return result


@app.get("/api/download/fnet/{doc_id}")
async def download_fnet_pdf(doc_id: int):
    """Proxy de download de PDF do FNET (resolve CORS)."""
    url = f"https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id={doc_id}"
    try:
        resp = requests.get(url, timeout=120, stream=True, headers={
            "User-Agent": BROWSER_HEADERS["User-Agent"],
            "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM",
        })
        resp.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(502, f"Erro ao baixar do FNET: {str(e)}")

    cd = resp.headers.get("Content-Disposition", "")
    match = re.findall(r'filename="?([^";\n]+)', cd)
    filename = match[0] if match else f"fnet_{doc_id}.pdf"

    content_type = resp.headers.get("Content-Type", "application/pdf")

    return StreamingResponse(
        io.BytesIO(resp.content),
        media_type=content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(resp.content)),
        },
    )


from fastapi.responses import HTMLResponse

@app.get("/api/fnet/{ticker}", response_class=HTMLResponse)
async def fnet_redirect(
    ticker: str,
    categoria: Optional[str] = Query(default="Relatórios", description="Categoria do documento"),
    tipo: Optional[str] = Query(default="Relatório Gerencial", description="Tipo do documento"),
):
    """
    Gera uma página que abre o FNET com CNPJ preenchido e seleciona 
    automaticamente Categoria=Relatórios e Tipo=Relatório Gerencial.
    
    Uso:
      - Padrão (Rel. Gerencial): /api/fnet/HGLG11
      - Todos os docs:           /api/fnet/HGLG11?categoria=&tipo=
      - Fatos relevantes:        /api/fnet/HGLG11?categoria=Fato Relevante&tipo=
    """
    ticker = ticker.upper().strip()
    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    nome = dados_fii.get("razao_social", ticker)

    if not cnpj_limpo:
        return HTMLResponse(f"""
        <html><body style="font-family:system-ui;padding:40px;text-align:center">
        <h2>CNPJ não encontrado para {ticker}</h2>
        <p>Tente buscar manualmente no <a href="https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCVM?paginaCertificados=false&tipoFundo=1">FNET</a></p>
        </body></html>
        """, status_code=404)

    # Mapeamento de categorias → nomes usados nos selects do FNET
    cat_label = categoria or ""
    tipo_label = tipo or ""
    filtro_desc = ""
    if cat_label and tipo_label:
        filtro_desc = f"{cat_label} → {tipo_label}"
    elif cat_label:
        filtro_desc = cat_label
    else:
        filtro_desc = "Todos os documentos"

    # A página usa JavaScript para:
    # 1. Abrir o FNET numa nova janela (ou iframe)
    # 2. Tentar preencher os selects via JS (funciona se same-origin, senão faz fallback)
    # 
    # O FNET aceita cnpjFundo como parâmetro GET na URL abrirGerenciadorDocumentosCVM
    # Isso preenche o campo CNPJ. Os selects de Categoria e Tipo precisam de interação JS.
    
    fnet_base = "https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM"
    fnet_url = f"{fnet_base}?cnpjFundo={cnpj_limpo}&tipoFundo=1"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>FNET — {ticker} — {filtro_desc}</title>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: system-ui, -apple-system, sans-serif; margin: 0; background: #f1f5f9; 
               display: flex; justify-content: center; align-items: center; min-height: 100vh; }}
        .card {{ text-align: center; padding: 32px 40px; background: white; border-radius: 16px;
                 box-shadow: 0 4px 24px rgba(0,0,0,0.08); max-width: 520px; width: 90%; }}
        .spinner {{ width: 36px; height: 36px; border: 3px solid #e2e8f0;
                    border-top: 3px solid #2563eb; border-radius: 50%;
                    animation: spin 0.8s linear infinite; margin: 0 auto 16px; }}
        @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
        .badge {{ display: inline-block; padding: 4px 14px; background: #059669; color: white;
                  border-radius: 20px; font-size: 12px; font-weight: 700; margin-bottom: 8px; }}
        .ticker {{ font-size: 28px; font-weight: 800; color: #0f172a; margin: 4px 0; }}
        .nome {{ color: #64748b; font-size: 14px; margin-bottom: 4px; }}
        .cnpj {{ color: #94a3b8; font-size: 13px; margin-bottom: 16px; }}
        .filtro {{ display: inline-block; padding: 4px 12px; background: #eff6ff; color: #2563eb;
                   border-radius: 6px; font-size: 12px; font-weight: 600; margin-bottom: 16px; }}
        .msg {{ color: #2563eb; font-size: 14px; font-weight: 500; }}
        .steps {{ text-align: left; background: #f8fafc; border-radius: 8px; padding: 16px;
                  margin-top: 16px; font-size: 13px; color: #475569; line-height: 1.7; }}
        .steps b {{ color: #0f172a; }}
        .btn {{ display: inline-block; padding: 10px 24px; background: #2563eb; color: white;
                border: none; border-radius: 8px; font-size: 14px; font-weight: 600;
                text-decoration: none; margin-top: 12px; cursor: pointer; }}
        .btn:hover {{ background: #1d4ed8; }}
        .link {{ color: #2563eb; font-size: 12px; margin-top: 12px; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="spinner" id="spinner"></div>
        <div class="badge">FII</div>
        <div class="ticker">{ticker}</div>
        <div class="nome">{nome}</div>
        <div class="cnpj">CNPJ: {cnpj}</div>
        <div class="filtro">📋 {filtro_desc}</div>
        <div class="msg" id="msg">Abrindo FNET com filtros...</div>
        
        <div class="steps" id="steps" style="display:none">
            <b>O FNET abrirá com o CNPJ preenchido.</b><br>
            Para ver os Relatórios Gerenciais:<br>
            1️⃣ Clique em <b>"EXIBIR FILTROS"</b><br>
            2️⃣ Em Categoria, selecione <b>"{cat_label or 'Todos'}"</b><br>
            3️⃣ Em Tipo, selecione <b>"{tipo_label or 'Todos'}"</b><br>
            4️⃣ Clique em <b>"Filtrar"</b>
        </div>

        <a class="btn" id="openBtn" href="{fnet_url}" target="_blank" style="display:none"
           onclick="document.getElementById('steps').style.display='block'">
            🔗 Abrir FNET
        </a>
        
        <div class="link" id="directLink" style="display:none">
            <a href="{fnet_url}" target="_blank">Abrir link direto do FNET →</a>
        </div>
    </div>

    <script>
        // Tentar abrir automaticamente 
        var fnetUrl = "{fnet_url}";
        var opened = false;
        
        setTimeout(function() {{
            try {{
                var w = window.open(fnetUrl, '_blank');
                if (w) {{
                    opened = true;
                    document.getElementById('msg').textContent = '✅ FNET aberto em nova aba!';
                    document.getElementById('spinner').style.display = 'none';
                    document.getElementById('steps').style.display = 'block';
                }}
            }} catch(e) {{}}
            
            if (!opened) {{
                // Popup blocked — show manual button
                document.getElementById('msg').textContent = 'Clique para abrir o FNET:';
                document.getElementById('spinner').style.display = 'none';
                document.getElementById('openBtn').style.display = 'inline-block';
            }}
            document.getElementById('directLink').style.display = 'block';
        }}, 600);
    </script>
</body>
</html>"""
    
    return HTMLResponse(html)


@app.get("/api/indicadores/{ticker}")
async def indicadores(ticker: str):
    """
    Busca indicadores de mercado para QUALQUER ativo (FII ou Ação).
    Fontes: Status Invest, Investidor10, Fundamentus (ações), Funds Explorer (FIIs).
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    cache_key = f"ind:{ticker}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    dados = buscar_indicadores(ticker, tipo)

    result = {
        "ticker": ticker,
        "tipo": tipo,
        "label": "FII" if tipo == "fii" else "Ação",
        **dados,
        "consultado_em": datetime.now().isoformat(),
    }

    cache_set(cache_key, result)
    return result


@app.get("/api/buscar/{ticker}")
async def busca_completa(
    ticker: str,
    max_docs: int = Query(default=20, ge=1, le=100),
):
    """
    Pesquisa completa unificada — documentos + indicadores.
    Funciona para FIIs E Ações.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    # Buscar documentos
    docs_result = await listar_documentos(ticker, max_docs=max_docs)

    # Buscar indicadores
    ind_result = await indicadores(ticker)

    return {
        "ticker": ticker,
        "tipo": tipo,
        "label": "FII" if tipo == "fii" else "Ação",
        "documentos": docs_result,
        "indicadores": ind_result,
        "consultado_em": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# CVM Dados Abertos — CSVs estruturados de FIIs
# ─────────────────────────────────────────────────────────────

CVM_CSV_CACHE: dict = {}
CVM_CSV_CACHE_TTL = 3600  # 1 hora

def _baixar_csv_cvm_fii(ano: int) -> dict:
    """
    Baixa o ZIP do informe mensal de FIIs da CVM e retorna
    um dict com as linhas de cada CSV dentro do ZIP.
    """
    cache_key = f"cvm_csv:{ano}"
    cached = CVM_CSV_CACHE.get(cache_key)
    if cached:
        ts, data = cached
        if time.time() - ts < CVM_CSV_CACHE_TTL:
            return data

    url = f"https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_{ano}.zip"
    
    try:
        resp = requests.get(url, timeout=90, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
        })
        resp.raise_for_status()
    except Exception as e:
        return {"erro": f"Erro ao baixar ZIP da CVM ({url}): {type(e).__name__}: {str(e)}"}

    if len(resp.content) < 100:
        return {"erro": f"ZIP muito pequeno ({len(resp.content)} bytes), possível erro de download"}

    resultado = {}
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for nome_arq in zf.namelist():
                if not nome_arq.endswith(".csv"):
                    continue
                with zf.open(nome_arq) as f:
                    raw = f.read()
                    # Tentar UTF-8 primeiro, fallback para latin-1
                    try:
                        texto = raw.decode("utf-8-sig")
                    except UnicodeDecodeError:
                        texto = raw.decode("latin-1", errors="replace")
                    
                    reader = csv.DictReader(io.StringIO(texto), delimiter=";")
                    rows = list(reader)
                    resultado[nome_arq] = rows
    except Exception as e:
        return {"erro": f"Erro ao processar ZIP: {type(e).__name__}: {str(e)}"}

    if resultado:
        CVM_CSV_CACHE[cache_key] = (time.time(), resultado)
    return resultado


def _filtrar_por_cnpj(rows: list, cnpj: str, cnpjs_classe: list = None) -> list:
    """
    Filtra linhas por CNPJ.
    1. Busca em colunas que contêm 'cnpj' no nome
    2. Fallback: busca padrão XX.XXX.XXX/XXXX-XX em qualquer coluna
    """
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    cnpjs_busca = {cnpj_limpo}
    if cnpjs_classe:
        for c in cnpjs_classe:
            cl = re.sub(r"\D", "", c) if c else ""
            if cl:
                cnpjs_busca.add(cl)
    
    if not cnpj_limpo:
        return []
    
    resultado = []
    for row in rows:
        found = False
        for col_name, col_val in row.items():
            col_clean = col_name.strip().replace("\ufeff", "").lower()
            if "cnpj" in col_clean:
                val_limpo = re.sub(r"\D", "", str(col_val or "").strip())
                if val_limpo in cnpjs_busca:
                    found = True
                    break
        
        # Fallback: buscar em qualquer coluna com valor parecido com CNPJ
        if not found:
            for col_name, col_val in row.items():
                val_str = str(col_val or "").strip()
                # Só checar valores que parecem CNPJ (14 dígitos ou XX.XXX.XXX/XXXX-XX)
                if "/" in val_str or len(re.sub(r"\D", "", val_str)) == 14:
                    val_limpo = re.sub(r"\D", "", val_str)
                    if val_limpo in cnpjs_busca:
                        found = True
                        break
        
        if found:
            resultado.append(row)
    
    return resultado


def _descobrir_cnpj_classe(csvs_por_ano: dict, cnpj_fundo: str, ticker: str = "") -> list:
    """
    Descobre TODOS os CNPJ_Fundo_Classe associados ao fundo.
    Coleta de TODOS os CSVs e anos disponíveis.
    """
    cnpj_limpo = re.sub(r"\D", "", cnpj_fundo)
    ticker_upper = ticker.upper().strip() if ticker else ""
    ticker_base = re.sub(r"\d+$", "", ticker_upper)
    cnpjs_encontrados = set()
    
    for ano in sorted(csvs_por_ano.keys(), reverse=True):
        csvs = csvs_por_ano[ano]
        for nome_csv, rows in csvs.items():
            if not rows:
                continue
            
            colunas = list(rows[0].keys())
            
            # Identificar colunas
            col_classe = None
            col_fundo = None
            
            for c in colunas:
                cl = c.lower()
                if "cnpj" in cl and "classe" in cl:
                    col_classe = c
                elif "cnpj" in cl and "fundo" in cl and "classe" not in cl and "admin" not in cl:
                    col_fundo = c
                elif cl == "cnpj" or cl == "cnpj_fundo":
                    col_fundo = c
            
            if not col_classe:
                continue
            
            # Se tem CNPJ_Fundo, buscar correspondência direta
            if col_fundo:
                for row in rows:
                    val = re.sub(r"\D", "", str(row.get(col_fundo, "")))
                    if val == cnpj_limpo:
                        cnpj_c = re.sub(r"\D", "", str(row.get(col_classe, "")))
                        if cnpj_c:
                            cnpjs_encontrados.add(cnpj_c)
            
            # Buscar ticker em qualquer valor
            if ticker_upper:
                for row in rows:
                    for c in colunas:
                        val = str(row.get(c, "")).upper()
                        if ticker_upper in val or (ticker_base and len(ticker_base) >= 3 and f" {ticker_base}" in f" {val}"):
                            cnpj_c = re.sub(r"\D", "", str(row.get(col_classe, "")))
                            if cnpj_c:
                                cnpjs_encontrados.add(cnpj_c)
                            break
    
    # Incluir o próprio CNPJ do fundo (pode ser = CNPJ_Fundo_Classe em formato antigo)
    cnpjs_encontrados.add(cnpj_limpo)
    return list(cnpjs_encontrados)


@app.get("/api/cvm/informe-mensal/{ticker}")
async def cvm_informe_mensal(
    ticker: str,
    ano: int = Query(default=None, description="Ano (ex: 2025). Default = ano atual"),
    limite: int = Query(default=6, ge=1, le=24, description="Últimos N meses"),
):
    """
    Retorna dados do Informe Mensal Estruturado de FII (direto da CVM/Dados Abertos).
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    
    if tipo != "fii":
        return {"ticker": ticker, "erro": "Informe mensal CVM disponível apenas para FIIs", "tipo": tipo}

    # Obter CNPJ
    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj")
    
    if not cnpj:
        return {"ticker": ticker, "erro": f"CNPJ não encontrado para {ticker}", "cadastro": dados_fii}

    # Determinar anos para tentar (atual + anteriores)
    ano_atual = datetime.now().year
    anos_tentar = [ano] if ano else [ano_atual, ano_atual - 1, ano_atual - 2]

    # Tentar cada ano até encontrar dados do fundo
    for a in anos_tentar:
        csvs = _baixar_csv_cvm_fii(a)
        if "erro" in csvs:
            continue

        # Verificar se o fundo existe em algum CSV deste ano
        tem_dados = False
        resultado_dados = {}
        
        for nome_csv, rows in csvs.items():
            filtrado = _filtrar_por_cnpj(rows, cnpj)
            if not filtrado:
                continue
            tem_dados = True

            # Ordenar por data de competência
            for row in filtrado:
                row["_dt"] = row.get("DT_COMPTC") or row.get("Data_Competencia") or row.get("Data_Referencia") or ""
            filtrado.sort(key=lambda r: r.get("_dt", ""), reverse=True)
            filtrado = filtrado[:limite]
            for row in filtrado:
                row.pop("_dt", None)

            nome_curto = nome_csv.replace(f"_{a}", "").replace("inf_mensal_fii_", "").replace(".csv", "")
            resultado_dados[nome_curto] = {
                "arquivo_csv": nome_csv,
                "total_registros": len(filtrado),
                "registros": filtrado,
            }

        if tem_dados:
            return {
                "ticker": ticker,
                "cnpj": cnpj,
                "cnpj_limpo": re.sub(r"\D", "", cnpj),
                "nome": dados_fii.get("razao_social", ""),
                "ano": a,
                "fonte": f"https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_{a}.zip",
                "dados": resultado_dados,
                "total_csvs_encontrados": len(resultado_dados),
                "consultado_em": datetime.now().isoformat(),
            }

    # Se nenhum ano funcionou, retornar debug detalhado
    # Baixar o ano mais recente novamente para debug
    ultimo_csvs = _baixar_csv_cvm_fii(anos_tentar[0])
    debug_csvs = {}
    if "erro" not in ultimo_csvs:
        for nome, rows in ultimo_csvs.items():
            colunas = list(rows[0].keys()) if rows else []
            cnpj_cols = [c for c in colunas if "cnpj" in c.lower()]
            amostra = "vazio"
            if rows and cnpj_cols:
                for col in cnpj_cols:
                    if "admin" not in col.lower() and rows[0].get(col):
                        amostra = rows[0][col]
                        break
            debug_csvs[nome] = {
                "total_linhas": len(rows),
                "colunas_cnpj": cnpj_cols,
                "matches_cnpj": len(_filtrar_por_cnpj(rows, cnpj)),
                "amostra_cnpj": amostra,
                "todas_colunas": colunas[:15],
            }

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "cnpj_limpo": re.sub(r"\D", "", cnpj),
        "nome": dados_fii.get("razao_social", ""),
        "anos_tentados": anos_tentar,
        "dados": {},
        "erro": f"Fundo {ticker} (CNPJ {cnpj}) não encontrado nos informes mensais dos anos {anos_tentar}",
        "debug_csvs": debug_csvs,
        "total_csvs_encontrados": 0,
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/lista-csvs")
async def cvm_lista_csvs(
    ano: int = Query(default=None, description="Ano (ex: 2025)")
):
    """Lista os CSVs disponíveis dentro do ZIP do informe mensal FII de um ano."""
    ano = ano or datetime.now().year
    csvs = _baixar_csv_cvm_fii(ano)
    
    if "erro" in csvs:
        return csvs

    info = {}
    for nome, rows in csvs.items():
        colunas = list(rows[0].keys()) if rows else []
        info[nome] = {
            "total_linhas": len(rows),
            "colunas": colunas,
            "amostra": rows[0] if rows else {},
        }

    return {
        "ano": ano,
        "url": f"https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_{ano}.zip",
        "csvs": info,
        "consultado_em": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# CVM Dados Abertos — Informe TRIMESTRAL Estruturado de FIIs
# Contém dados de imóveis: endereço, ABL, locatários, etc.
# ─────────────────────────────────────────────────────────────

CVM_TRIM_CACHE: dict = {}
CVM_TRIM_CACHE_TTL = 3600  # 1 hora


def _baixar_csv_cvm_fii_trimestral(ano: int) -> dict:
    """
    Baixa o ZIP do informe trimestral de FIIs da CVM e retorna
    um dict com as linhas de cada CSV dentro do ZIP.
    """
    cache_key = f"cvm_trim:{ano}"
    cached = CVM_TRIM_CACHE.get(cache_key)
    if cached:
        ts, data = cached
        if time.time() - ts < CVM_TRIM_CACHE_TTL:
            return data

    url = f"https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/inf_trimestral_fii_{ano}.zip"

    try:
        resp = requests.get(url, timeout=120, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
        })
        resp.raise_for_status()
    except Exception as e:
        return {"erro": f"Erro ao baixar ZIP trimestral da CVM ({url}): {type(e).__name__}: {str(e)}"}

    if len(resp.content) < 100:
        return {"erro": f"ZIP muito pequeno ({len(resp.content)} bytes)"}

    resultado = {}
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for nome_arq in zf.namelist():
                if not nome_arq.endswith(".csv"):
                    continue
                with zf.open(nome_arq) as f:
                    raw = f.read()
                    try:
                        texto = raw.decode("utf-8-sig")
                    except UnicodeDecodeError:
                        texto = raw.decode("latin-1", errors="replace")

                    reader = csv.DictReader(io.StringIO(texto), delimiter=";")
                    rows = list(reader)
                    resultado[nome_arq] = rows
    except Exception as e:
        return {"erro": f"Erro ao processar ZIP trimestral: {type(e).__name__}: {str(e)}"}

    if resultado:
        CVM_TRIM_CACHE[cache_key] = (time.time(), resultado)
    return resultado


@app.get("/api/cvm/informe-trimestral/{ticker}")
async def cvm_informe_trimestral(
    ticker: str,
    limite: int = Query(default=4, ge=1, le=20, description="Qtd de trimestres"),
    ano: Optional[int] = Query(default=None, description="Ano específico (omitir = automático)"),
):
    """
    Busca dados do informe trimestral de um FII na CVM.
    Contém dados detalhados de imóveis, ativos, endereços, etc.
    
    URL: https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/inf_trimestral_fii_{ano}.zip
    """
    ticker = ticker.upper().strip()
    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado para este ticker."}

    ano_atual = datetime.now().year
    anos_tentar = [ano] if ano else [ano_atual, ano_atual - 1, ano_atual - 2]

    for a in anos_tentar:
        csvs = _baixar_csv_cvm_fii_trimestral(a)
        if "erro" in csvs:
            continue

        tem_dados = False
        resultado_dados = {}

        for nome_csv, rows in csvs.items():
            filtrado = _filtrar_por_cnpj(rows, cnpj)
            if not filtrado:
                continue
            tem_dados = True

            # Ordenar por data (vários nomes possíveis)
            for row in filtrado:
                row["_dt"] = (
                    row.get("DT_COMPTC") or row.get("Data_Competencia")
                    or row.get("Data_Referencia") or row.get("DT_REF") or ""
                )
            filtrado.sort(key=lambda r: r.get("_dt", ""), reverse=True)
            
            # Limitar por TRIMESTRES distintos (não por registros)
            # Ex: limite=4 = últimos 4 trimestres (cada um pode ter N imóveis)
            datas_distintas = sorted(set(r["_dt"] for r in filtrado if r["_dt"]), reverse=True)
            datas_permitidas = set(datas_distintas[:limite])
            if datas_permitidas:
                filtrado = [r for r in filtrado if r.get("_dt", "") in datas_permitidas]
            
            for row in filtrado:
                row.pop("_dt", None)

            nome_curto = nome_csv.replace(f"_{a}", "").replace("inf_trimestral_fii_", "").replace(".csv", "")
            resultado_dados[nome_curto] = {
                "arquivo_csv": nome_csv,
                "total_registros": len(filtrado),
                "colunas": list(filtrado[0].keys()) if filtrado else [],
                "registros": filtrado,
            }

        if tem_dados:
            return {
                "ticker": ticker,
                "cnpj": cnpj,
                "cnpj_limpo": re.sub(r"\D", "", cnpj),
                "nome": dados_fii.get("razao_social", ""),
                "ano": a,
                "tipo": "informe_trimestral",
                "fonte": f"https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/inf_trimestral_fii_{a}.zip",
                "dados": resultado_dados,
                "total_csvs_encontrados": len(resultado_dados),
                "consultado_em": datetime.now().isoformat(),
            }

    # Nenhum ano funcionou — debug
    ultimo_csvs = _baixar_csv_cvm_fii_trimestral(anos_tentar[0])
    debug_csvs = {}
    if "erro" not in ultimo_csvs:
        for nome, rows in ultimo_csvs.items():
            colunas = list(rows[0].keys()) if rows else []
            cnpj_cols = [c for c in colunas if "cnpj" in c.lower()]
            amostra = "vazio"
            if rows and cnpj_cols:
                for col in cnpj_cols:
                    if "admin" not in col.lower() and rows[0].get(col):
                        amostra = rows[0][col]
                        break
            debug_csvs[nome] = {
                "total_linhas": len(rows),
                "colunas_cnpj": cnpj_cols,
                "matches_cnpj": len(_filtrar_por_cnpj(rows, cnpj)),
                "amostra_cnpj": amostra,
                "todas_colunas": colunas[:20],
            }

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "cnpj_limpo": re.sub(r"\D", "", cnpj),
        "anos_tentados": anos_tentar,
        "dados": {},
        "erro": f"Fundo {ticker} não encontrado nos informes trimestrais dos anos {anos_tentar}",
        "debug_csvs": debug_csvs,
        "total_csvs_encontrados": 0,
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/lista-csvs-trimestral")
async def cvm_lista_csvs_trimestral(ano: Optional[int] = None):
    """Lista os CSVs disponíveis dentro do ZIP do informe trimestral FII de um ano."""
    ano = ano or datetime.now().year
    csvs = _baixar_csv_cvm_fii_trimestral(ano)

    if "erro" in csvs:
        return csvs

    info = {}
    for nome, rows in csvs.items():
        colunas = list(rows[0].keys()) if rows else []
        info[nome] = {
            "total_linhas": len(rows),
            "colunas": colunas,
            "amostra": rows[0] if rows else {},
        }

    return {
        "ano": ano,
        "url": f"https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/inf_trimestral_fii_{ano}.zip",
        "csvs": info,
        "consultado_em": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# CVM Dados Abertos — Ofertas Públicas de Distribuição
# IPOs, Follow-ons, Esforços Restritos de FIIs e Ações
# ─────────────────────────────────────────────────────────────

CVM_OFERTAS_CACHE: dict = {}
CVM_OFERTAS_CACHE_TTL = 3600 * 6  # 6 horas (atualizado diariamente)


def _baixar_ofertas_cvm() -> dict:
    """
    Baixa o ZIP de ofertas públicas de distribuição da CVM.
    Contém IPOs, follow-ons, esforços restritos de todos os emissores.
    URL: https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip
    """
    cache_key = "cvm_ofertas"
    cached = CVM_OFERTAS_CACHE.get(cache_key)
    if cached:
        ts, data = cached
        if time.time() - ts < CVM_OFERTAS_CACHE_TTL:
            return data

    url = "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"

    try:
        resp = requests.get(url, timeout=120, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
        })
        resp.raise_for_status()
    except Exception as e:
        return {"erro": f"Erro ao baixar ZIP de ofertas da CVM: {type(e).__name__}: {str(e)}"}

    if len(resp.content) < 100:
        return {"erro": f"ZIP muito pequeno ({len(resp.content)} bytes)"}

    resultado = {}
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for nome_arq in zf.namelist():
                if not nome_arq.endswith(".csv"):
                    continue
                with zf.open(nome_arq) as f:
                    raw = f.read()
                    try:
                        texto = raw.decode("utf-8-sig")
                    except UnicodeDecodeError:
                        texto = raw.decode("latin-1", errors="replace")

                    reader = csv.DictReader(io.StringIO(texto), delimiter=";")
                    rows = list(reader)
                    resultado[nome_arq] = rows
    except Exception as e:
        return {"erro": f"Erro ao processar ZIP de ofertas: {type(e).__name__}: {str(e)}"}

    if resultado:
        CVM_OFERTAS_CACHE[cache_key] = (time.time(), resultado)
    return resultado


def _filtrar_ofertas_por_cnpj(rows: list, cnpj: str) -> list:
    """Filtra ofertas por CNPJ do emissor."""
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    resultado = []
    for row in rows:
        # Tentar várias colunas possíveis
        row_cnpj = ""
        for col in ("CNPJ_Emissor", "CNPJ_EMISSOR", "cnpj_emissor", "CNPJ"):
            if col in row and row[col]:
                row_cnpj = row[col]
                break
        # Também tentar por case-insensitive
        if not row_cnpj:
            for k, v in row.items():
                if "cnpj" in k.lower() and "emissor" in k.lower() and v:
                    row_cnpj = v
                    break

        row_cnpj_limpo = re.sub(r"\D", "", row_cnpj)
        if row_cnpj_limpo == cnpj_limpo:
            resultado.append(row)
    return resultado


def _filtrar_ofertas_por_nome(rows: list, nome_busca: str) -> list:
    """Filtra ofertas pelo nome do emissor (busca parcial)."""
    nome_upper = nome_busca.upper()
    resultado = []
    for row in rows:
        nome_emissor = ""
        for col in ("Nome_Emissor", "NOME_EMISSOR", "Emissor"):
            if col in row and row[col]:
                nome_emissor = row[col]
                break
        if not nome_emissor:
            for k, v in row.items():
                if "emissor" in k.lower() and "nome" in k.lower() and v:
                    nome_emissor = v
                    break
        if nome_upper in nome_emissor.upper():
            resultado.append(row)
    return resultado


@app.get("/api/cvm/emissoes/{ticker}")
async def cvm_emissoes(
    ticker: str,
):
    """
    Retorna histórico de emissões (IPO, follow-on, esforços restritos) de um FII ou ação.
    
    Fonte: https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip
    Atualização: diária
    
    Busca por CNPJ do emissor nos CSVs oferta_distribuicao.csv e oferta_resolucao_160.csv
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    # Obter CNPJ
    if tipo == "fii":
        dados_fii = descobrir_dados_fii(ticker)
        cnpj = dados_fii.get("cnpj", "")
        nome = dados_fii.get("razao_social", "")
    else:
        # Para ações, não temos CNPJ local, tentar pelo nome
        cnpj = ""
        nome = ticker

    # Baixar dados de ofertas
    csvs = _baixar_ofertas_cvm()
    if "erro" in csvs:
        return {"ticker": ticker, "erro": csvs["erro"]}

    todas_ofertas = []

    for nome_csv, rows in csvs.items():
        ofertas = []
        if cnpj:
            ofertas = _filtrar_ofertas_por_cnpj(rows, cnpj)
        
        # Se não encontrou por CNPJ, tentar por nome
        if not ofertas and nome:
            ofertas = _filtrar_ofertas_por_nome(rows, nome)
        
        # Se não encontrou por nome completo, tentar pelo ticker sem número
        if not ofertas:
            ticker_base = re.sub(r"\d+[BF]?$", "", ticker)
            if ticker_base:
                ofertas = _filtrar_ofertas_por_nome(rows, ticker_base)

        for oferta in ofertas:
            oferta["_fonte_csv"] = nome_csv
            todas_ofertas.append(oferta)

    # Ordenar por data (mais recente primeiro)
    def _get_data_oferta(row):
        for col in ("Data_Registro", "Data_Inicio_Oferta", "Data_Inicio_Distribuicao", "Data_Protocolo"):
            val = row.get(col, "")
            if val and len(val) >= 8:
                return val
        # Buscar qualquer coluna com "data" no nome
        for k, v in row.items():
            if "data" in k.lower() and v and len(str(v)) >= 8:
                return str(v)
        return ""
    
    todas_ofertas.sort(key=lambda r: _get_data_oferta(r), reverse=True)

    # Calcular preço por cota quando possível
    for oferta in todas_ofertas:
        # Tentar extrair volume e quantidade para calcular preço
        volume = None
        quantidade = None
        
        for col in ("Volume_Oferta", "Volume_Total", "Montante_Total", "Volume_Emissao"):
            val = oferta.get(col, "")
            if val:
                try:
                    volume = float(str(val).replace(",", "."))
                    break
                except:
                    pass
        # Fallback: qualquer coluna com "volume" ou "montante"
        if volume is None:
            for k, v in oferta.items():
                if ("volume" in k.lower() or "montante" in k.lower()) and v:
                    try:
                        volume = float(str(v).replace(",", "."))
                        break
                    except:
                        pass

        for col in ("Quantidade_Valores_Mobiliarios", "Quantidade_Total", "Qtd_Valores_Mobiliarios", "Qtd_Cotas"):
            val = oferta.get(col, "")
            if val:
                try:
                    quantidade = float(str(val).replace(",", "."))
                    break
                except:
                    pass
        if quantidade is None:
            for k, v in oferta.items():
                if ("quantidade" in k.lower() or "qtd" in k.lower()) and "valores" in k.lower() and v:
                    try:
                        quantidade = float(str(v).replace(",", "."))
                        break
                    except:
                        pass

        if volume and quantidade and quantidade > 0:
            oferta["_preco_calculado"] = round(volume / quantidade, 4)
            oferta["_volume"] = volume
            oferta["_quantidade"] = quantidade

    # ── Enriquecer com campos computados ──

    # Agrupar por CNPJ para detectar oferta inicial
    ofertas_por_cnpj = {}
    for oferta in todas_ofertas:
        for col in ("CNPJ_Emissor", "CNPJ_EMISSOR", "cnpj_emissor"):
            if col in oferta and oferta[col]:
                c = re.sub(r"\D", "", oferta[col])
                if c not in ofertas_por_cnpj:
                    ofertas_por_cnpj[c] = []
                ofertas_por_cnpj[c].append(oferta)
                break

    for oferta in todas_ofertas:
        # 1. Data de encerramento
        data_enc = ""
        for col in ("Data_Encerramento_Oferta", "Data_Encerramento", 
                     "Data_Fim_Oferta", "Data_Encerramento_Distribuicao"):
            val = oferta.get(col, "")
            if val and len(str(val)) >= 8:
                data_enc = val
                break
        oferta["_data_encerramento"] = data_enc

        # 2. Oferta inicial (IPO) vs follow-on
        # Se é a oferta mais antiga daquele CNPJ, é a inicial
        oferta_data = _get_data_oferta(oferta)
        eh_inicial = False
        for col in ("CNPJ_Emissor", "CNPJ_EMISSOR", "cnpj_emissor"):
            if col in oferta and oferta[col]:
                c = re.sub(r"\D", "", oferta[col])
                lista = ofertas_por_cnpj.get(c, [])
                if lista:
                    # Pegar a data mais antiga
                    datas = [_get_data_oferta(o) for o in lista if _get_data_oferta(o)]
                    if datas:
                        mais_antiga = min(datas)
                        if oferta_data == mais_antiga:
                            eh_inicial = True
                break
        
        # Também checar campo Tipo_Oferta (PRIMÁRIA = emissão primária de novas cotas)
        tipo_oferta = ""
        for col in ("Tipo_Oferta", "Tipo_Distribuicao"):
            val = oferta.get(col, "")
            if val:
                tipo_oferta = str(val).strip()
                break
        
        oferta["_oferta_inicial"] = eh_inicial
        oferta["_tipo_oferta"] = tipo_oferta  # PRIMÁRIA, SECUNDÁRIA, MISTA
        oferta["_numero_emissao"] = None  # Será preenchido abaixo

        # 3. Volume captado
        # Tentar campos específicos de volume captado/realizado
        vol_captado = None
        for col in ("Valor_Total_Registrado", "Valor_Total_Oferta", 
                     "Montante_Captado", "Volume_Captado", "Valor_Total_Total",
                     "Valor_Total"):
            val = oferta.get(col, "")
            if val:
                try:
                    vol_captado = float(str(val).replace(",", "."))
                    if vol_captado > 0:
                        break
                except:
                    pass
        
        # Fallback: buscar qualquer coluna com "valor_total" ou "montante"
        if not vol_captado:
            for k, v in oferta.items():
                kl = k.lower()
                if ("valor_total" in kl or "montante" in kl or "volume" in kl) and v:
                    try:
                        val = float(str(v).replace(",", "."))
                        if val > 0:
                            vol_captado = val
                            break
                    except:
                        pass
        
        oferta["_volume_captado"] = vol_captado

        # Modalidade (400, 476, 160)
        modalidade = ""
        for col in ("Modalidade_Oferta", "Modalidade_Registro", "Modalidade"):
            val = oferta.get(col, "")
            if val:
                modalidade = str(val).strip()
                break
        oferta["_modalidade"] = modalidade

    # Numerar emissões por CNPJ (1ª, 2ª, 3ª...) 
    for c, lista in ofertas_por_cnpj.items():
        lista_ord = sorted(lista, key=lambda o: _get_data_oferta(o) or "9999")
        for i, oferta in enumerate(lista_ord, 1):
            oferta["_numero_emissao"] = i

    return {
        "ticker": ticker,
        "cnpj": cnpj or "N/A",
        "tipo": tipo,
        "total_emissoes": len(todas_ofertas),
        "emissoes": todas_ofertas,
        "fonte": "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip",
        "nota": "Campos _prefixados são calculados: _oferta_inicial (1ª emissão do CNPJ), _volume_captado (melhor estimativa), _data_encerramento, _numero_emissao (ordem cronológica)",
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/emissoes/{ticker}/detalhe/{numero}")
async def cvm_emissao_detalhe(ticker: str, numero: int):
    """
    Mostra TODOS os campos preenchidos de uma emissão específica.
    numero = _numero_emissao (1, 2, 3...)
    """
    # Reusar o endpoint principal
    resultado = await cvm_emissoes(ticker)
    if "erro" in resultado:
        return resultado
    
    emissoes = resultado.get("emissoes", [])
    
    # Encontrar a emissão pelo número
    emissao = None
    for e in emissoes:
        if e.get("_numero_emissao") == numero:
            emissao = e
            break
    
    if not emissao:
        return {"erro": f"Emissão #{numero} não encontrada", "total": len(emissoes)}
    
    # Separar campos preenchidos vs vazios
    preenchidos = {}
    vazios = []
    for k, v in emissao.items():
        v_str = str(v).strip() if v is not None else ""
        if v_str and v_str not in ("", "0", "0.0", "0.00", "None", "0.0000"):
            preenchidos[k] = v
        else:
            vazios.append(k)
    
    # Destacar campos financeiros
    campos_financeiros = {k: v for k, v in preenchidos.items() 
                          if any(x in k.lower() for x in ["valor", "volume", "montante", "preco", "qtd", "quantidade", "total", "captado"])}
    
    return {
        "ticker": ticker.upper(),
        "emissao_numero": numero,
        "campos_preenchidos": preenchidos,
        "campos_financeiros": campos_financeiros,
        "campos_vazios": vazios,
        "total_preenchidos": len(preenchidos),
        "total_vazios": len(vazios),
    }


@app.get("/api/cvm/emissoes-debug")
async def cvm_emissoes_debug():
    """
    Debug: mostra os CSVs disponíveis no ZIP de ofertas,
    suas colunas e uma amostra de dados de FII.
    """
    csvs = _baixar_ofertas_cvm()
    if "erro" in csvs:
        return csvs

    info = {}
    for nome, rows in csvs.items():
        colunas = list(rows[0].keys()) if rows else []
        
        # Encontrar amostras que sejam FII
        amostras_fii = []
        for row in rows:
            for k, v in row.items():
                if v and ("FII" in str(v).upper() or "IMOBILI" in str(v).upper()):
                    amostras_fii.append(row)
                    break
            if len(amostras_fii) >= 3:
                break

        info[nome] = {
            "total_linhas": len(rows),
            "colunas": colunas,
            "amostra_geral": rows[0] if rows else {},
            "amostras_fii": amostras_fii,
        }

    return {
        "url": "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip",
        "csvs": info,
        "consultado_em": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# CVM Dados Abertos — Primeiro Informe Mensal + Histórico VP
# Busca o primeiro registro do fundo desde 2016
# ─────────────────────────────────────────────────────────────

@app.get("/api/cvm/primeiro-informe/{ticker}")
async def cvm_primeiro_informe(ticker: str):
    """
    Busca o PRIMEIRO informe mensal de um FII na CVM (desde 2016).
    Útil para comparar valor patrimonial no início vs atual.
    
    Percorre anos de 2016 até o atual procurando o primeiro registro.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year

    # Pré-carregar todos os ZIPs em paralelo
    
    anos = list(range(2016, ano_atual + 1))
    csvs_por_ano = {}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_baixar_csv_cvm_fii, ano): ano for ano in anos}
        for future in as_completed(futures):
            ano = futures[future]
            try:
                resultado = future.result()
                if "erro" not in resultado:
                    csvs_por_ano[ano] = resultado
            except Exception:
                pass

    # Percorrer do mais antigo ao mais recente
    for ano in sorted(csvs_por_ano.keys()):
        csvs = csvs_por_ano[ano]

        # Priorizar CSV "geral" que tem PL, cotas, VP
        csvs_ordenados = sorted(
            csvs.items(),
            key=lambda x: (0 if "geral" in x[0].lower() else 1)
        )

        # Buscar em TODOS os CSVs e consolidar dados
        dados_consolidados = {}
        data_mais_antiga = None
        csv_fonte = None

        for nome_csv, rows in csvs_ordenados:
            if not rows:
                continue
            filtrado = _filtrar_por_cnpj(rows, cnpj)
            if not filtrado:
                continue

            # Ordenar por data (mais antigo primeiro)
            for row in filtrado:
                row["_dt"] = (
                    row.get("DT_COMPTC") or row.get("Data_Competencia")
                    or row.get("Data_Referencia") or row.get("DT_REF") or ""
                )
            filtrado.sort(key=lambda r: r.get("_dt", ""))
            primeiro = filtrado[0]
            dt = primeiro.get("_dt", "")

            if data_mais_antiga is None or (dt and dt < data_mais_antiga):
                data_mais_antiga = dt

            # Coletar todos os campos com valor do primeiro registro
            for k, v in primeiro.items():
                if k == "_dt":
                    continue
                if v and v != "" and v != "None" and v != "0" and v != "0.00":
                    if k not in dados_consolidados:
                        dados_consolidados[k] = v
                        if csv_fonte is None:
                            csv_fonte = nome_csv

            # Limpar _dt
            for row in filtrado:
                row.pop("_dt", None)

        if not dados_consolidados:
            continue

        # Extrair PL, cotas e calcular VP
        pl = None
        cotas = None
        nr_cotistas = None

        for k, v in dados_consolidados.items():
            kl = k.lower()
            try:
                val = float(str(v).replace(",", "."))
            except:
                continue

            # Patrimônio Líquido
            if pl is None and (
                ("patrim" in kl and "liq" in kl)
                or k in ("VL_PATRIM_LIQ", "Patrimonio_Liquido", "Total_Patrimonio_Liquido")
            ):
                pl = val

            # Quantidade de Cotas (não confundir com cotistas)
            if cotas is None and (
                k in ("QT_COTAS", "NR_COTAS_EMITIDAS", "Quantidade_Cotas_Emitidas")
                or ("cota" in kl and ("emitida" in kl or "qt" in kl or "quant" in kl) and "cotist" not in kl)
            ):
                if val is not None and val > 1000:  # cotas, não cotistas
                    cotas = val

            # Cotistas
            if "cotist" in kl or k in ("NR_COTST", "Numero_Cotistas"):
                nr_cotistas = val

        vp_cota = None
        if pl and cotas and cotas > 0:
            vp_cota = round(pl / cotas, 4)

        return {
            "ticker": ticker,
            "cnpj": cnpj,
            "nome": dados_fii.get("razao_social", ""),
            "primeiro_informe": {
                "ano": ano,
                "data_competencia": data_mais_antiga,
                "dados": dados_consolidados,
                "patrimonio_liquido": pl,
                "quantidade_cotas": cotas,
                "numero_cotistas": nr_cotistas,
                "vp_cota_calculado": vp_cota,
            },
            "consultado_em": datetime.now().isoformat(),
        }

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "erro": f"Nenhum informe mensal encontrado para {ticker} entre 2016 e {ano_atual}",
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/historico-vp/{ticker}")
async def cvm_historico_vp(ticker: str):
    """
    Retorna o valor patrimonial por cota (VP) ao longo do tempo.
    Busca o PRIMEIRO e o ÚLTIMO informe mensal, mais pontos intermediários.
    Útil para montar gráfico de evolução do VP vs preço de emissão.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year
    pontos = []  # Lista de {data, pl, cotas, vp_cota}

    # Download paralelo dos ZIPs da CVM
    
    anos = list(range(2016, ano_atual + 1))
    csvs_por_ano = {}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_baixar_csv_cvm_fii, ano): ano for ano in anos}
        for future in as_completed(futures):
            ano = futures[future]
            try:
                resultado = future.result()
                if "erro" not in resultado:
                    csvs_por_ano[ano] = resultado
            except Exception:
                pass

    for ano in sorted(csvs_por_ano.keys()):
        csvs = csvs_por_ano[ano]

        # Priorizar CSV "geral" que tem PL e cotas
        csvs_ordenados = sorted(
            csvs.items(),
            key=lambda x: (0 if "geral" in x[0].lower() else 1)
        )

        encontrou_neste_ano = False
        for nome_csv, rows in csvs_ordenados:
            if encontrou_neste_ano:
                break
            nome_lower = nome_csv.lower()
            # Focar no CSV geral
            if "geral" not in nome_lower:
                continue

            filtrado = _filtrar_por_cnpj(rows, cnpj)
            if not filtrado:
                continue

            for row in filtrado:
                dt = (
                    row.get("DT_COMPTC") or row.get("Data_Competencia")
                    or row.get("Data_Referencia") or row.get("DT_REF") or ""
                )
                if not dt:
                    continue

                # Extrair PL e Cotas
                pl = None
                cotas = None
                nr_cotistas = None

                for k, v in row.items():
                    if not v or v in ("0", "0.00", "None", ""):
                        continue
                    kl = k.lower()
                    try:
                        val = float(str(v).replace(",", "."))
                    except:
                        continue

                    if pl is None and (
                        ("patrim" in kl and "liq" in kl)
                        or k in ("VL_PATRIM_LIQ", "Patrimonio_Liquido", "Total_Patrimonio_Liquido")
                    ):
                        pl = val

                    if cotas is None and (
                        k in ("QT_COTAS", "NR_COTAS_EMITIDAS", "Quantidade_Cotas_Emitidas")
                        or ("cota" in kl and ("emitida" in kl or "qt" in kl or "quant" in kl) and "cotist" not in kl)
                    ):
                        if val is not None and val > 1000:
                            cotas = val

                    if "cotist" in kl or k in ("NR_COTST", "Numero_Cotistas"):
                        nr_cotistas = val

                if pl and cotas and cotas > 0:
                    vp = round(pl / cotas, 4)
                    pontos.append({
                        "data": dt,
                        "patrimonio_liquido": pl,
                        "quantidade_cotas": cotas,
                        "numero_cotistas": nr_cotistas,
                        "vp_cota": vp,
                        "ano": ano,
                    })
                    encontrou_neste_ano = True

    # Remover duplicatas por data e ordenar
    vistos = set()
    pontos_unicos = []
    for p in pontos:
        if p["data"] not in vistos:
            vistos.add(p["data"])
            pontos_unicos.append(p)
    pontos_unicos.sort(key=lambda p: p["data"])

    primeiro = pontos_unicos[0] if pontos_unicos else None
    ultimo = pontos_unicos[-1] if pontos_unicos else None

    variacao_vp = None
    if primeiro and ultimo and primeiro["vp_cota"] > 0:
        variacao_vp = round(
            ((ultimo["vp_cota"] - primeiro["vp_cota"]) / primeiro["vp_cota"]) * 100, 2
        )

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "total_pontos": len(pontos_unicos),
        "primeiro": primeiro,
        "ultimo": ultimo,
        "variacao_vp_percentual": variacao_vp,
        "historico": pontos_unicos,
        "consultado_em": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# FNET — Proventos: Rendimentos e Amortizações (valor por cota)
# Busca documentos estruturados no FNET tipo "Rendimentos/Amortizações"
# ─────────────────────────────────────────────────────────────

def _buscar_proventos_fnet(cnpj: str, max_docs: int = 200) -> list:
    """
    Busca documentos de Rendimentos/Amortizações no FNET.
    idCategoriaDocumento=6 ou idTipoDocumento=4 filtra por "Rendimentos e Amortizações".
    """
    cnpj_limpo = re.sub(r"\D", "", cnpj) if cnpj else ""
    if not cnpj_limpo:
        return []

    headers = {
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://fnet.bmfbovespa.com.br",
        "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCVM?paginaCertificados=false&tipoFundo=1",
    }

    url = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"

    # Tentar diferentes combinações de filtro para pegar rendimentos/amortizações
    tentativas = [
        # Tipo específico "Rendimentos e Amortizações" 
        {"idCategoriaDocumento": "6", "idTipoDocumento": "4", "idEspecieDocumento": "0"},
        {"idCategoriaDocumento": "6", "idTipoDocumento": "0", "idEspecieDocumento": "0"},
        {"idCategoriaDocumento": "0", "idTipoDocumento": "4", "idEspecieDocumento": "0"},
        # Aviso aos Cotistas (estruturado)
        {"idCategoriaDocumento": "14", "idTipoDocumento": "41", "idEspecieDocumento": "0"},
        {"idCategoriaDocumento": "14", "idTipoDocumento": "0", "idEspecieDocumento": "0"},
    ]

    for tentativa in tentativas:
        payload = {
            "d": "0",
            "s": "0",
            "l": str(max_docs),
            "o[0][dataEntrega]": "desc",
            "tipoFundo": "1",
            "situacao": "A",
            "cnpj": cnpj_limpo,
            "dataInicial": "",
            "dataFinal": "",
            "idFundo": "0",
            "razaoSocial": "",
            "codigoNegociacao": "",
            **tentativa,
        }

        try:
            resp = requests.post(url, data=payload, headers=headers, timeout=20)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    docs = data.get("data", [])
                    if docs:
                        return docs
                except (ValueError, KeyError):
                    continue
        except Exception:
            continue

    return []


def _parse_provento_html(doc_id: int) -> dict | None:
    """
    Faz download do documento FNET e extrai campos estruturados.
    O documento de Rendimentos/Amortizações tem formato HTML com campos nomeados.
    """
    url = f"https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={doc_id}&cvm=true"
    
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        
        html = resp.text
        
        # Extrair campos do HTML estruturado
        resultado = {"doc_id": doc_id}
        
        # Tipo do evento (Rendimento ou Amortização)
        # Procurar padrões como "Tipo do Evento" seguido de "Rendimento" ou "Amortização"
        
        # Abordagem: buscar por texto no HTML
        html_lower = html.lower()
        
        # Detectar tipo
        if "amortização" in html_lower or "amortizacao" in html_lower:
            # Verificar se é checkbox marcado
            # No HTML do FNET, o tipo marcado tem "X" ou "checked"
            idx_amort = html_lower.find("amortização")
            if idx_amort == -1:
                idx_amort = html_lower.find("amortizacao")
            
            # Verificar contexto: se tem "X" perto antes de "Amortização"
            contexto = html[max(0, idx_amort-100):idx_amort+50]
            if "X" in contexto or "x" in contexto.lower().replace("amortização", "").replace("amortizacao", ""):
                resultado["tipo"] = "AMORTIZACAO"
            elif "rendimento" in html_lower:
                # Ambos presentes, verificar qual está marcado
                idx_rend = html_lower.find("rendimento")
                contexto_rend = html[max(0, idx_rend-100):idx_rend+50]
                if "X" in contexto_rend:
                    resultado["tipo"] = "RENDIMENTO"
                else:
                    resultado["tipo"] = "AMORTIZACAO"
            else:
                resultado["tipo"] = "AMORTIZACAO"
        elif "rendimento" in html_lower:
            resultado["tipo"] = "RENDIMENTO"
        else:
            resultado["tipo"] = "DESCONHECIDO"
        
        # Extrair valor por cota
        # Padrão: "Valor do provento por cota" seguido de "R$ X,XX"
        re_mod = re  # Already imported at top
        
        # Valor por cota
        match_valor = re_mod.search(
            r'(?:valor\s+(?:do\s+)?provento\s+por\s+cota|valor\s+por\s+cota)[^R$]*R\$\s*([\d.,]+)',
            html, re_mod.IGNORECASE
        )
        if match_valor:
            resultado["valor_por_cota"] = float(match_valor.group(1).replace(".", "").replace(",", "."))
        else:
            # Tentar padrão mais genérico
            match_valor2 = re_mod.search(r'R\$\s*([\d]+[.,][\d]+)', html)
            if match_valor2:
                val_str = match_valor2.group(1).replace(".", "").replace(",", ".")
                try:
                    resultado["valor_por_cota"] = float(val_str)
                except:
                    pass
        
        # Data-base (último dia com direito)
        match_data_base = re_mod.search(
            r'(?:data[\s-]*base|ltimo\s+dia\s+de\s+negocia|data.*com.*direito)[^0-9]*([\d]{2}/[\d]{2}/[\d]{4})',
            html, re_mod.IGNORECASE
        )
        if match_data_base:
            resultado["data_base"] = match_data_base.group(1)
        
        # Data pagamento
        match_pgto = re_mod.search(
            r'(?:data\s+(?:do\s+)?pagamento|data.*pagamento)[^0-9]*([\d]{2}/[\d]{2}/[\d]{4})',
            html, re_mod.IGNORECASE
        )
        if match_pgto:
            resultado["data_pagamento"] = match_pgto.group(1)
        
        # Período de referência
        match_periodo = re_mod.search(
            r'(?:per[ií]odo\s+de\s+refer[eê]ncia)[^A-Z]*([\w]+/[\d]{4})',
            html, re_mod.IGNORECASE
        )
        if match_periodo:
            resultado["periodo_referencia"] = match_periodo.group(1)
        
        # ISIN
        match_isin = re_mod.search(r'(BR[\w]{10})', html)
        if match_isin:
            resultado["isin"] = match_isin.group(1)
        
        # Isenção IR
        if "isento" in html_lower or "isenção" in html_lower or "isencao" in html_lower:
            resultado["isento_ir"] = True
        
        return resultado if resultado.get("valor_por_cota") else None
        
    except Exception as e:
        return {"doc_id": doc_id, "erro": str(e)}


@app.get("/api/fii/proventos/{ticker}")
async def fii_proventos(ticker: str, max_docs: int = 200):
    """
    Retorna histórico de rendimentos e amortizações de um FII.
    
    Fonte: FNET/B3 — documentos estruturados "Rendimentos e Amortizações".
    Cada registro contém: tipo (RENDIMENTO ou AMORTIZACAO), valor por cota,
    data-base, data de pagamento.
    
    Separar amortizações dos rendimentos permite calcular VP ajustado com precisão.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    # Buscar documentos de rendimentos/amortizações no FNET
    docs = _buscar_proventos_fnet(cnpj, max_docs=max_docs)
    
    if not docs:
        return {
            "ticker": ticker,
            "cnpj": cnpj,
            "erro": "Nenhum documento de rendimentos/amortizações encontrado no FNET",
            "nota": "Possível que o filtro de categoria/tipo não esteja correto. Verificar IDs.",
        }

    # Para cada documento, fazer parse do HTML para extrair dados estruturados
    rendimentos = []
    amortizacoes = []
    erros = []
    
    for doc in docs:
        doc_id = doc.get("id")
        if not doc_id:
            continue
        
        descricao = doc.get("descricaoCategoria", "") + " " + doc.get("descricaoTipo", "")
        data_entrega = doc.get("dataEntrega", "")
        
        provento = _parse_provento_html(doc_id)
        
        if provento and provento.get("valor_por_cota"):
            provento["data_entrega_fnet"] = data_entrega
            provento["descricao_fnet"] = descricao.strip()
            
            if provento.get("tipo") == "AMORTIZACAO":
                amortizacoes.append(provento)
            else:
                rendimentos.append(provento)
        elif provento and provento.get("erro"):
            erros.append(provento)

    # Calcular totais
    total_amortizado = sum(a.get("valor_por_cota", 0) for a in amortizacoes)
    total_rendimentos = sum(r.get("valor_por_cota", 0) for r in rendimentos)

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "resumo": {
            "total_proventos": len(rendimentos) + len(amortizacoes),
            "total_rendimentos": len(rendimentos),
            "total_amortizacoes": len(amortizacoes),
            "valor_total_rendimentos": round(total_rendimentos, 4),
            "valor_total_amortizacoes": round(total_amortizado, 4),
            "valor_total_proventos": round(total_rendimentos + total_amortizado, 4),
        },
        "amortizacoes": amortizacoes,
        "rendimentos_recentes": rendimentos[:12],  # Últimos 12 meses
        "total_rendimentos_disponivel": len(rendimentos),
        "erros_parse": erros[:5] if erros else [],
        "docs_fnet_encontrados": len(docs),
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/fii/debug-fnet-tipos/{ticker}")
async def debug_fnet_tipos(ticker: str):
    """
    Debug: busca TODOS os documentos de um FII no FNET (sem filtro de tipo)
    e lista as categorias/tipos disponíveis. Útil para descobrir IDs corretos.
    """
    ticker = ticker.upper().strip()
    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    cnpj_limpo = re.sub(r"\D", "", cnpj) if cnpj else ""
    
    if not cnpj_limpo:
        return {"erro": "CNPJ não encontrado"}

    headers = {
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://fnet.bmfbovespa.com.br",
        "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCVM?paginaCertificados=false&tipoFundo=1",
    }

    url = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"

    # Buscar SEM filtro de tipo/categoria
    payload = {
        "d": "0", "s": "0", "l": "50",
        "o[0][dataEntrega]": "desc",
        "tipoFundo": "1",
        "idCategoriaDocumento": "0",
        "idTipoDocumento": "0",
        "idEspecieDocumento": "0",
        "situacao": "A",
        "cnpj": cnpj_limpo,
        "dataInicial": "", "dataFinal": "",
        "idFundo": "0",
        "razaoSocial": "", "codigoNegociacao": "",
    }

    try:
        resp = requests.post(url, data=payload, headers=headers, timeout=20)
        if resp.status_code != 200:
            return {"erro": f"FNET retornou status {resp.status_code}", "body_preview": resp.text[:200]}
        
        # Verificar se é JSON
        content_type = resp.headers.get("Content-Type", "")
        if "json" not in content_type and "javascript" not in content_type:
            return {"erro": f"FNET retornou {content_type}, não JSON", "body_preview": resp.text[:300]}
        
        try:
            data = resp.json()
        except Exception as je:
            return {"erro": f"JSON parse error: {str(je)}", "body_preview": resp.text[:300]}
        
        docs = data.get("data", [])
        total = data.get("recordsTotal", 0)
    except Exception as e:
        return {"erro": str(e)}

    # Agrupar por categoria + tipo
    tipos_encontrados = {}
    docs_resumo = []
    
    for doc in docs:
        cat = doc.get("categoriaDocumento", "?")
        tipo = doc.get("tipoDocumento", "?")
        desc_cat = doc.get("descricaoCategoria", "?")
        desc_tipo = doc.get("descricaoTipo", "?")
        id_cat = doc.get("idCategoriaDocumento", "?")
        id_tipo = doc.get("idTipoDocumento", "?")
        
        chave = f"{id_cat}:{id_tipo}"
        if chave not in tipos_encontrados:
            tipos_encontrados[chave] = {
                "idCategoriaDocumento": id_cat,
                "idTipoDocumento": id_tipo,
                "categoriaDocumento": cat or desc_cat,
                "tipoDocumento": tipo or desc_tipo,
                "descricaoCategoria": desc_cat,
                "descricaoTipo": desc_tipo,
                "count": 0,
                "exemplo_id": doc.get("id"),
            }
        tipos_encontrados[chave]["count"] += 1
        
        docs_resumo.append({
            "id": doc.get("id"),
            "descricao": f"{desc_cat} - {desc_tipo}",
            "data": doc.get("dataEntrega", ""),
            "id_cat": id_cat,
            "id_tipo": id_tipo,
        })

    return {
        "ticker": ticker,
        "cnpj": cnpj_limpo,
        "total_docs_disponiveis": total,
        "docs_retornados": len(docs),
        "tipos_encontrados": list(tipos_encontrados.values()),
        "docs_resumo": docs_resumo,
        "consultado_em": datetime.now().isoformat(),
    }




# ─────────────────────────────────────────────────────────────
# CVM — Evolução Patrimonial (composição ativos/passivos no tempo)
# Snapshots: primeiro disponível + dez/cada ano + último disponível
# ─────────────────────────────────────────────────────────────

def _extrair_float_safe(valor) -> float:
    """Converte string numérica para float, retornando 0.0 se inválido."""
    if not valor or valor in ("0", "0.00", "None", "", "0,00"):
        return 0.0
    try:
        return float(str(valor).replace(",", "."))
    except (ValueError, TypeError):
        return 0.0


def _extrair_composicao(row: dict) -> dict:
    """
    Extrai composição de ativos e passivos de uma linha do CSV ativo_passivo.
    Retorna dict com ativos e passivos, todos os valores numéricos (0.0 se ausente).
    """
    ativos = {}
    passivos = {}
    indicadores = {}

    # Mapeamento direto de colunas → nomes padronizados
    map_ativos = {
        "Total_Necessidades_Liquidas": "caixa_liquidez",
        "Disponibilidades": "disponibilidades",
        "Fundos_Renda_Fixa": "fundos_renda_fixa",
        "Titulos_Publicos": "titulos_publicos",
        "Titulos_Privados": "titulos_privados",
        "Total_Investido": "_total_investido",
        "Imoveis_Renda_Acabados": "imoveis_renda",
        "Imoveis_Renda_Construcao": "imoveis_construcao",
        "Imoveis_Venda_Acabados": "imoveis_venda",
        "Terrenos": "terrenos",
        "FII": "cotas_fii",
        "FIP": "cotas_fip",
        "FIDC": "fidc",
        "CRI": "cri",
        "CRA": "cra",
        "LCI": "lci",
        "LCA": "lca",
        "LIG": "lig",
        "LH": "lh",
        "Acoes": "acoes",
        "Debentures": "debentures",
        "Letras_Hipotecarias": "letras_hipotecarias",
        "Certificados_Deposito_Valores_Mobiliarios": "cdvm",
        "Outros_Valores_Mobiliarios": "outros_vm",
        "Valores_Receber": "valores_receber",
        "Contas_Receber_Aluguel": "receber_aluguel",
        "Contas_Receber_Venda_Imoveis": "receber_venda_imoveis",
        "Outros_Valores_Receber": "outros_receber",
        "Obrigacoes_Aquisicao_Imoveis": "obrig_aquisicao_imoveis",
    }

    map_passivos = {
        "Total_Passivo": "_total_passivo",
        "Rendimentos_Distribuir": "rendimentos_distribuir",
        "Taxa_Administracao_Pagar": "taxa_adm_pagar",
        "Taxa_Performance_Pagar": "taxa_performance_pagar",
        "Outros_Valores_Pagar": "outros_pagar",
    }

    map_indicadores = {
        "Percentual_Amortizacao_Cotas_Mes": "amortizacao_pct_mes",
        "Percentual_Dividend_Yield_Mes": "dy_mes",
        "Percentual_Rentabilidade_Efetiva_Mes": "rentabilidade_efetiva_mes",
        "Percentual_Rentabilidade_Patrimonial_Mes": "rentabilidade_patrimonial_mes",
    }

    # Campos de PL/cotas que podem aparecer no ativo_passivo também
    map_gerais = {
        "Total_Patrimonio_Liquido": "patrimonio_liquido",
        "Patrimonio_Liquido": "patrimonio_liquido",
        "Total_Necessidades_Liquidas_Registros": "patrimonio_liquido",
        "Quantidade_Cotas_Emitidas": "cotas_emitidas",
        "Cotas_Sociedades_FII": "cotas_fii",
        "Total_Numero_Cotistas": "cotistas",
        "Numero_Cotistas": "cotistas",
        "Registros": "cotistas",
    }

    gerais = {}

    for k, v in row.items():
        val = _extrair_float_safe(v)
        if val == 0.0:
            continue
        if k in map_ativos:
            ativos[map_ativos[k]] = val
        elif k in map_passivos:
            passivos[map_passivos[k]] = val
        elif k in map_indicadores:
            indicadores[map_indicadores[k]] = val
        elif k in map_gerais:
            campo = map_gerais[k]
            if campo not in gerais:
                gerais[campo] = val

    resultado = {}
    if ativos:
        resultado["ativos"] = ativos
    if passivos:
        resultado["passivos"] = passivos
    if indicadores:
        resultado["indicadores"] = indicadores
    if gerais:
        resultado["gerais"] = gerais
    return resultado


def _extrair_dados_gerais(row: dict) -> dict:
    """
    Extrai PL, cotistas, cotas, DY, rentabilidade do CSV complemento ou geral.
    Usa mapeamento direto, sem comparações genéricas arriscadas.
    """
    dados = {}

    # Mapeamento direto coluna → campo
    map_direto = {
        # PL — todas as variantes conhecidas
        "VL_PATRIM_LIQ": "patrimonio_liquido",
        "Patrimonio_Liquido": "patrimonio_liquido",
        "Total_Patrimonio_Liquido": "patrimonio_liquido",
        "Total_Necessidades_Liquidas": "patrimonio_liquido",
        # Cotas — todas as variantes
        "QT_COTAS": "cotas_emitidas",
        "NR_COTAS_EMITIDAS": "cotas_emitidas",
        "Quantidade_Cotas_Emitidas": "cotas_emitidas",
        "Cotas_Emitidas": "cotas_emitidas",
        "Quantidade_Cotas": "cotas_emitidas",
        # Cotistas — todas as variantes
        "NR_COTST": "cotistas",
        "Total_Cotistas": "cotistas",
        "Numero_Cotistas": "cotistas",
        "Total_Numero_Cotistas": "cotistas",
        "Numero_Total_Cotistas": "cotistas",
        # Cotistas PF
        "Numero_Cotistas_Pessoa_Fisica": "cotistas_pf",
        # Indicadores
        "Percentual_Dividend_Yield_Mes": "dy_mes",
        "Percentual_Rentabilidade_Efetiva_Mes": "rentabilidade_efetiva_mes",
        "Percentual_Rentabilidade_Patrimonial_Mes": "rentabilidade_patrimonial_mes",
        "Percentual_Amortizacao_Cotas_Mes": "amortizacao_pct_mes",
        # Valor do ativo
        "Valor_Ativo": "valor_ativo",
        "Valor_Patrimonial_Cotas_Emitidas": "vp_total_cotas",
        # Rendimentos
        "Rendimentos_Distribuir": "rendimentos_distribuir",
        "Percentual_Despesas_Taxa_Administracao": "taxa_adm_pct",
        "Percentual_Despesas_Agente_Custodiante": "taxa_custodia_pct",
    }

    for k, v in row.items():
        if k in map_direto:
            val = _extrair_float_safe(v)
            if val != 0.0:
                campo = map_direto[k]
                if campo not in dados:
                    dados[campo] = val
        else:
            # Fallback fuzzy para campos críticos não mapeados
            kl = k.lower()
            val = _extrair_float_safe(v)
            if val == 0.0:
                continue
            # PL: qualquer campo com "patrim" + "liq"
            if "patrimonio_liquido" not in dados and "patrim" in kl and "liq" in kl:
                dados["patrimonio_liquido"] = val
            # Cotas emitidas: campo com "cota" + "emitid" e valor grande
            elif "cotas_emitidas" not in dados and "cota" in kl and "emitid" in kl and "cotist" not in kl and val > 100:
                dados["cotas_emitidas"] = val
            # Cotistas: campo com "cotist" + ("total" ou "numero")
            elif "cotistas" not in dados and "cotist" in kl and ("total" in kl or "numero" in kl) and val < 10000000:
                dados["cotistas"] = val

    # Calcular VP/cota
    pl = dados.get("patrimonio_liquido")
    cotas = dados.get("cotas_emitidas")
    if pl and cotas and cotas > 0:
        dados["vp_cota"] = round(pl / cotas, 4)

    return dados


@app.get("/api/cvm/evolucao-patrimonial/{ticker}")
async def cvm_evolucao_patrimonial(ticker: str, ano_inicio: int = 2016):
    """
    Retorna snapshots da composição patrimonial ao longo do tempo.

    Parâmetros:
    - ano_inicio: ano inicial (default 2016). Use 2020+ para carregamento rápido.

    Para cada snapshot: composição de ativos, passivos, PL, cotas, VP/cota, DY.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year
    todos_snapshots = {}

    # Download paralelo dos ZIPs da CVM
    anos = list(range(ano_inicio, ano_atual + 1))
    csvs_por_ano = {}

    with ThreadPoolExecutor(max_workers=min(len(anos), 10)) as executor:
        futures = {executor.submit(_baixar_csv_cvm_fii, ano): ano for ano in anos}
        for future in as_completed(futures):
            ano = futures[future]
            try:
                resultado = future.result()
                if "erro" not in resultado:
                    csvs_por_ano[ano] = resultado
            except Exception:
                pass

    for ano in sorted(csvs_por_ano.keys()):
        csvs = csvs_por_ano[ano]

        # Priorizar CSVs: ativo_passivo → complemento → geral
        csvs_ordenados = sorted(
            csvs.items(),
            key=lambda x: (
                0 if "ativo_passivo" in x[0].lower() else
                1 if "complemento" in x[0].lower() else
                2 if "geral" in x[0].lower() else 3
            )
        )

        for nome_csv, rows in csvs_ordenados:
            if not rows:
                continue

            nome_lower = nome_csv.lower()
            is_ativo_passivo = "ativo_passivo" in nome_lower
            is_complemento = "complemento" in nome_lower
            is_geral = "geral" in nome_lower

            if not (is_ativo_passivo or is_complemento or is_geral):
                continue

            filtrado = _filtrar_por_cnpj(rows, cnpj)
            if not filtrado:
                continue

            for row in filtrado:
                dt = (
                    row.get("DT_COMPTC") or row.get("Data_Competencia")
                    or row.get("Data_Referencia") or row.get("DT_REF") or ""
                )
                if not dt or len(dt) < 7:
                    continue

                periodo = dt[:7]

                if periodo not in todos_snapshots:
                    todos_snapshots[periodo] = {
                        "periodo": periodo,
                        "data": dt,
                        "composicao": None,
                        "dados_gerais": None,
                    }

                snap = todos_snapshots[periodo]

                if is_ativo_passivo:
                    novos = _extrair_composicao(row)
                    if novos:
                        if snap["composicao"] is None:
                            snap["composicao"] = novos
                        else:
                            for k2, v2 in novos.items():
                                if k2 not in snap["composicao"]:
                                    snap["composicao"][k2] = v2
                        
                        # Usar campos gerais do ativo_passivo como fallback
                        gerais_ap = novos.get("gerais", {})
                        if gerais_ap:
                            if snap["dados_gerais"] is None:
                                snap["dados_gerais"] = dict(gerais_ap)
                            else:
                                for k2, v2 in gerais_ap.items():
                                    if k2 not in snap["dados_gerais"]:
                                        snap["dados_gerais"][k2] = v2

                if is_complemento or is_geral:
                    novos = _extrair_dados_gerais(row)
                    if novos:
                        if snap["dados_gerais"] is None:
                            snap["dados_gerais"] = novos
                        else:
                            for k2, v2 in novos.items():
                                if k2 not in snap["dados_gerais"]:
                                    snap["dados_gerais"][k2] = v2

    if not todos_snapshots:
        return {
            "ticker": ticker,
            "cnpj": cnpj,
            "erro": f"Nenhum dado encontrado para {ticker} desde {ano_inicio}",
        }

    # Ordenar períodos
    periodos_ordenados = sorted(todos_snapshots.keys())

    # Selecionar snapshots: primeiro + dezembros + último
    selecionados = []
    periodos_incluidos = set()

    # 1. Primeiro disponível
    primeiro_periodo = periodos_ordenados[0]
    selecionados.append(todos_snapshots[primeiro_periodo])
    periodos_incluidos.add(primeiro_periodo)

    # 2. Dezembro de cada ano (ou último mês disponível do ano)
    anos_disponiveis = sorted(set(p[:4] for p in periodos_ordenados))
    for a in anos_disponiveis:
        dez = f"{a}-12"
        if dez in todos_snapshots and dez not in periodos_incluidos:
            selecionados.append(todos_snapshots[dez])
            periodos_incluidos.add(dez)
        else:
            meses_ano = [p for p in periodos_ordenados if p.startswith(a)]
            if meses_ano:
                ultimo_mes = meses_ano[-1]
                if ultimo_mes not in periodos_incluidos:
                    selecionados.append(todos_snapshots[ultimo_mes])
                    periodos_incluidos.add(ultimo_mes)

    # 3. Último disponível
    ultimo_periodo = periodos_ordenados[-1]
    if ultimo_periodo not in periodos_incluidos:
        selecionados.append(todos_snapshots[ultimo_periodo])

    selecionados.sort(key=lambda s: s["periodo"])

    # Pós-processamento: garantir VP/cota combinando todas as fontes
    for snap in selecionados:
        dg = snap.get("dados_gerais") or {}
        comp = snap.get("composicao") or {}
        gerais_ap = comp.get("gerais", {})
        
        # Merge gerais do ativo_passivo no dados_gerais
        if gerais_ap:
            if snap["dados_gerais"] is None:
                snap["dados_gerais"] = {}
                dg = snap["dados_gerais"]
            for k2, v2 in gerais_ap.items():
                if k2 not in dg:
                    dg[k2] = v2
        
        # Calcular VP/cota se ausente
        if dg and "vp_cota" not in dg:
            pl = dg.get("patrimonio_liquido")
            cotas = dg.get("cotas_emitidas")
            if pl and cotas and cotas > 0:
                dg["vp_cota"] = round(pl / cotas, 4)

    # Calcular resumo
    primeiro = selecionados[0]
    ultimo = selecionados[-1]

    resumo = {
        "periodo_inicio": primeiro["periodo"],
        "periodo_fim": ultimo["periodo"],
        "total_snapshots": len(selecionados),
        "total_periodos_disponiveis": len(periodos_ordenados),
    }

    # VP/cota
    dg_inicio = primeiro.get("dados_gerais") or {}
    dg_fim = ultimo.get("dados_gerais") or {}
    vp_inicio = dg_inicio.get("vp_cota")
    vp_fim = dg_fim.get("vp_cota")

    if vp_inicio and vp_fim and vp_inicio > 0:
        resumo["vp_cota_inicio"] = vp_inicio
        resumo["vp_cota_fim"] = vp_fim
        resumo["variacao_vp_pct"] = round(((vp_fim - vp_inicio) / vp_inicio) * 100, 2)

    # PL
    pl_inicio = dg_inicio.get("patrimonio_liquido")
    pl_fim = dg_fim.get("patrimonio_liquido")
    if pl_inicio and pl_fim and pl_inicio > 0:
        resumo["pl_inicio"] = pl_inicio
        resumo["pl_fim"] = pl_fim
        resumo["variacao_pl_pct"] = round(((pl_fim - pl_inicio) / pl_inicio) * 100, 2)

    # Cotistas
    cot_inicio = dg_inicio.get("cotistas")
    cot_fim = dg_fim.get("cotistas")
    if cot_inicio and cot_fim:
        resumo["cotistas_inicio"] = cot_inicio
        resumo["cotistas_fim"] = cot_fim

    # Cotas emitidas
    cotas_inicio = dg_inicio.get("cotas_emitidas")
    cotas_fim = dg_fim.get("cotas_emitidas")
    if cotas_inicio and cotas_fim:
        resumo["cotas_inicio"] = cotas_inicio
        resumo["cotas_fim"] = cotas_fim

    # Total investido
    comp_inicio = (primeiro.get("composicao") or {}).get("ativos", {})
    comp_fim = (ultimo.get("composicao") or {}).get("ativos", {})
    ti_inicio = comp_inicio.get("_total_investido")
    ti_fim = comp_fim.get("_total_investido")
    if ti_inicio and ti_fim and ti_inicio > 0:
        resumo["total_investido_inicio"] = ti_inicio
        resumo["total_investido_fim"] = ti_fim
        resumo["variacao_investido_pct"] = round(((ti_fim - ti_inicio) / ti_inicio) * 100, 2)

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "resumo": resumo,
        "snapshots": selecionados,
        "consultado_em": datetime.now().isoformat(),
    }



# ─────────────────────────────────────────────────────────────
# CVM — Endpoints Estruturados: DRE, Balanço, Imóveis, Cotistas
# Fonte: Informe Trimestral + Anual
# ─────────────────────────────────────────────────────────────

def _download_trimestral_parallel(ano_inicio: int, ano_fim: int) -> dict:
    """Download paralelo dos ZIPs trimestrais."""
    anos = list(range(ano_inicio, ano_fim + 1))
    resultado = {}
    with ThreadPoolExecutor(max_workers=min(len(anos), 10)) as executor:
        futures = {executor.submit(_baixar_csv_cvm_fii_trimestral_explore, a): a for a in anos}
        for future in as_completed(futures):
            ano = futures[future]
            try:
                r = future.result()
                if "erro" not in r:
                    resultado[ano] = r
            except Exception:
                pass
    return resultado


def _download_anual_parallel(ano_inicio: int, ano_fim: int) -> dict:
    """Download paralelo dos ZIPs anuais."""
    anos = list(range(ano_inicio, ano_fim + 1))
    resultado = {}
    with ThreadPoolExecutor(max_workers=min(len(anos), 10)) as executor:
        futures = {executor.submit(_baixar_csv_cvm_fii_anual, a): a for a in anos}
        for future in as_completed(futures):
            ano = futures[future]
            try:
                r = future.result()
                if "erro" not in r:
                    resultado[ano] = r
            except Exception:
                pass
    return resultado


def _extrair_contabilidade(rows: list, cnpj: str, cnpjs_classe: list = None) -> list:
    """Extrai e estrutura dados contábeis de um CSV de contabilidade."""
    filtrado = _filtrar_por_cnpj(rows, cnpj, cnpjs_classe)
    resultados = []
    for row in filtrado:
        dt = row.get("Data_Referencia") or row.get("DT_COMPTC") or ""
        registro = {"data_referencia": dt}
        for k, v in row.items():
            if k.startswith("CNPJ") or k == "Data_Referencia" or k.startswith("DT_"):
                continue
            val = _extrair_float_safe(v)
            if val != 0.0:
                registro[k] = val
            elif v and v not in ("0", "0.00", "None", "", "0,00"):
                registro[k] = v
        if len(registro) > 1:
            resultados.append(registro)
    return resultados


@app.get("/api/cvm/contabilidade/{ticker}")
async def cvm_contabilidade(
    ticker: str,
    ano_inicio: int = Query(default=2016, description="Ano inicial (default: 2016, início dos dados CVM)"),
    ano_fim: int = Query(default=None, description="Ano final (default: atual)"),
):
    """
    Retorna DRE e Balanço Patrimonial contábil de um FII.

    Fonte: Informe Trimestral + Anual da CVM.
    Dados: receitas, despesas, resultado líquido, ativos, passivos, PL contábil.

    Trimestral: dados por trimestre (mais granular).
    Anual: dados consolidados do exercício.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year
    if not ano_fim:
        ano_fim = ano_atual

    # Download paralelo trimestrais
    trimestrais = _download_trimestral_parallel(ano_inicio, ano_fim)

    # Descobrir CNPJ_Fundo_Classe (Resolução CVM 175)
    cnpjs_classe = _descobrir_cnpj_classe(trimestrais, cnpj, ticker)

    dre_trimestral = []
    balanco_ativo_trimestral = []
    balanco_passivo_trimestral = []

    # Processar trimestrais — buscar em TODOS os CSVs
    for ano in sorted(trimestrais.keys()):
        csvs = trimestrais[ano]
        for nome_csv, rows in csvs.items():
            if not rows:
                continue
            nl = nome_csv.lower()
            
            # Pular CSVs que claramente não são financeiros
            skip = ["alienacao", "terreno", "aquisicao"]
            if any(s in nl for s in skip):
                continue
            
            dados = _extrair_contabilidade(rows, cnpj, cnpjs_classe)
            
            if not dados:
                continue

            if "dre" in nl or "rentabilidade_efetiva" in nl or "resultado" in nl:
                for d in dados:
                    d["_fonte"] = "trimestral"
                    d["_csv"] = nome_csv
                dre_trimestral.extend(dados)
            elif "ativo" in nl and "passivo" not in nl:
                for d in dados:
                    d["_fonte"] = "trimestral"
                    d["_csv"] = nome_csv
                balanco_ativo_trimestral.extend(dados)
            elif "passivo" in nl:
                for d in dados:
                    d["_fonte"] = "trimestral"
                    d["_csv"] = nome_csv
                balanco_passivo_trimestral.extend(dados)
            else:
                # CSV desconhecido com dados — incluir na DRE por padrão
                for d in dados:
                    d["_fonte"] = "trimestral"
                    d["_csv"] = nome_csv
                dre_trimestral.extend(dados)

    # Processar anuais — removido por enquanto (foco trimestral)

    # Ordenar por data
    for lista in [dre_trimestral, balanco_ativo_trimestral, balanco_passivo_trimestral]:
        lista.sort(key=lambda x: x.get("data_referencia", ""))

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "periodo": f"{ano_inicio}-{ano_fim}",
        "dre": dre_trimestral,
        "balanco": {
            "ativo": balanco_ativo_trimestral,
            "passivo": balanco_passivo_trimestral,
        },
        "resumo": {
            "trimestres_dre": len(dre_trimestral),
            "trimestres_balanco_ativo": len(balanco_ativo_trimestral),
            "trimestres_balanco_passivo": len(balanco_passivo_trimestral),
        },
        "consultado_em": datetime.now().isoformat(),
    }



@app.get("/api/cvm/imoveis/{ticker}")
async def cvm_imoveis(
    ticker: str,
    ano_inicio: int = Query(default=2016, description="Ano inicial (default: 2016, início dos dados CVM)"),
    ano_fim: int = Query(default=None, description="Ano final (default: atual)"),
):
    """
    Retorna lista de imóveis de um FII com dados operacionais.

    Fonte: Informe Trimestral da CVM.
    Dados: endereço, área, vacância, inadimplência, receita, contratos de aluguel.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year
    if not ano_fim:
        ano_fim = ano_atual

    trimestrais = _download_trimestral_parallel(ano_inicio, ano_fim)

    # Descobrir CNPJ_Fundo_Classe (Resolução CVM 175)
    cnpjs_classe = _descobrir_cnpj_classe(trimestrais, cnpj, ticker)

    imoveis_renda = []
    imoveis_construcao = []
    imoveis_venda = []
    alugueis = []
    terrenos = []

    for ano in sorted(trimestrais.keys()):
        csvs = trimestrais[ano]
        for nome_csv, rows in csvs.items():
            if not rows:
                continue
            nl = nome_csv.lower()

            filtrado = _filtrar_por_cnpj(rows, cnpj, cnpjs_classe)
            if not filtrado:
                continue

            for row in filtrado:
                dt = row.get("Data_Referencia") or row.get("DT_COMPTC") or ""
                registro = {"data_referencia": dt, "_ano": ano}

                for k, v in row.items():
                    if k.startswith("CNPJ") or k == "Data_Referencia" or k.startswith("DT_"):
                        continue
                    val = _extrair_float_safe(v)
                    if val != 0.0:
                        registro[k] = val
                    elif v and v not in ("0", "0.00", "None", "", "0,00"):
                        registro[k] = v

                if len(registro) <= 2:
                    continue

                if "imovel_renda_acabado" in nl or "renda_acabados" in nl:
                    imoveis_renda.append(registro)
                elif "imovel_renda_construcao" in nl or "construcao" in nl:
                    imoveis_construcao.append(registro)
                elif "imovel_venda" in nl or "venda_acabados" in nl:
                    imoveis_venda.append(registro)
                elif "alugue" in nl:
                    alugueis.append(registro)
                elif "terreno" in nl:
                    terrenos.append(registro)

    # Ordenar por data
    for lista in [imoveis_renda, imoveis_construcao, imoveis_venda, alugueis, terrenos]:
        lista.sort(key=lambda x: x.get("data_referencia", ""))

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "periodo": f"{ano_inicio}-{ano_fim}",
        "imoveis_renda": imoveis_renda,
        "imoveis_construcao": imoveis_construcao,
        "imoveis_venda": imoveis_venda,
        "alugueis": alugueis,
        "terrenos": terrenos,
        "resumo": {
            "total_imoveis_renda": len(imoveis_renda),
            "total_imoveis_construcao": len(imoveis_construcao),
            "total_imoveis_venda": len(imoveis_venda),
            "total_alugueis": len(alugueis),
            "total_terrenos": len(terrenos),
        },
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/cotistas/{ticker}")
async def cvm_cotistas(
    ticker: str,
    ano_inicio: int = Query(default=2016, description="Ano inicial (default: 2016, início dos dados CVM)"),
    ano_fim: int = Query(default=None, description="Ano final (default: atual)"),
):
    """
    Retorna distribuição de cotistas por faixa de participação.

    Fonte: Informe Anual da CVM.
    Dados: número de cotistas por faixa (0-5%, 5-10%, etc.), PF vs PJ.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year
    if not ano_fim:
        ano_fim = ano_atual

    anuais = _download_anual_parallel(ano_inicio, ano_fim)

    # Descobrir CNPJ_Fundo_Classe (Resolução CVM 175)
    cnpjs_classe = _descobrir_cnpj_classe(anuais, cnpj, ticker)

    distribuicoes = []

    for ano in sorted(anuais.keys()):
        csvs = anuais[ano]
        for nome_csv, rows in csvs.items():
            if not rows:
                continue
            nl = nome_csv.lower()
            if "cotist" not in nl and "distribuicao" not in nl:
                continue

            filtrado = _filtrar_por_cnpj(rows, cnpj, cnpjs_classe)
            for row in filtrado:
                dt = row.get("Data_Referencia") or ""
                registro = {"data_referencia": dt, "_ano": ano}
                for k, v in row.items():
                    if k.startswith("CNPJ") or k == "Data_Referencia":
                        continue
                    val = _extrair_float_safe(v)
                    if val != 0.0:
                        registro[k] = val
                    elif v and v not in ("0", "0.00", "None", "", "0,00"):
                        registro[k] = v
                if len(registro) > 2:
                    distribuicoes.append(registro)

    distribuicoes.sort(key=lambda x: x.get("data_referencia", ""))

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "periodo": f"{ano_inicio}-{ano_fim}",
        "distribuicao_cotistas": distribuicoes,
        "total_registros": len(distribuicoes),
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/governanca/{ticker}")
async def cvm_governanca(
    ticker: str,
    ano: int = Query(default=None, description="Ano (default: mais recente)"),
):
    """
    Retorna dados de governança e relatório do administrador.

    Fonte: Informe Anual da CVM.
    Dados: administrador, gestor, custodiante, auditor, relatório texto.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year
    if not ano:
        ano = ano_atual - 1  # Anual geralmente disponível ano anterior

    # Tentar ano solicitado e anterior
    anuais = _download_anual_parallel(ano - 1, ano)

    # Descobrir CNPJ_Fundo_Classe (Resolução CVM 175)
    cnpjs_classe = _descobrir_cnpj_classe(anuais, cnpj, ticker)

    governanca = []
    relatorio_administrador = []
    geral = []

    for a in sorted(anuais.keys(), reverse=True):  # Mais recente primeiro
        csvs = anuais[a]
        for nome_csv, rows in csvs.items():
            if not rows:
                continue
            nl = nome_csv.lower()

            filtrado = _filtrar_por_cnpj(rows, cnpj, cnpjs_classe)
            if not filtrado:
                continue

            for row in filtrado:
                dt = row.get("Data_Referencia") or ""
                registro = {"data_referencia": dt, "_ano": a}
                for k, v in row.items():
                    if k.startswith("CNPJ") or k == "Data_Referencia":
                        continue
                    val = _extrair_float_safe(v)
                    if val != 0.0:
                        registro[k] = val
                    elif v and v not in ("0", "0.00", "None", "", "0,00"):
                        registro[k] = v

                if len(registro) <= 2:
                    continue

                if "governanca" in nl:
                    governanca.append(registro)
                elif "geral" in nl:
                    geral.append(registro)

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "ano_referencia": ano,
        "governanca": governanca,
        "relatorio_geral": geral,
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/transacoes/{ticker}")

@app.get("/api/cvm/transacoes/{ticker}")
async def cvm_transacoes(
    ticker: str,
    ano_inicio: int = Query(default=2016, description="Ano inicial"),
    ano_fim: int = Query(default=None, description="Ano final"),
):
    """
    Histórico de aquisições e alienações de ativos — Informe Anual CVM.
    CSVs: inf_anual_fii_ativo_adquirido / inf_anual_fii_ativo_vendido
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    if tipo != "fii":
        return {"ticker": ticker, "erro": "Disponível apenas para FIIs", "tipo": tipo}

    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"ticker": ticker, "erro": "CNPJ não encontrado"}

    ano_atual = datetime.now().year
    if not ano_fim:
        ano_fim = ano_atual

    cnpj_limpo = re.sub(r"\D", "", cnpj)

    try:
        anuais = _download_anual_parallel(ano_inicio, ano_fim)
    except Exception as e:
        return {"ticker": ticker, "erro": f"Falha ao baixar anuais: {e}",
                "timeline": [], "resumo": {"total_transacoes": 0}}

    if not anuais:
        return {"ticker": ticker, "cnpj": cnpj, "timeline": [],
                "resumo": {"total_transacoes": 0, "total_aquisicoes": 0,
                           "total_alienacoes": 0, "volume_compras": 0,
                           "volume_vendas": 0, "saldo_liquido": 0},
                "consultado_em": datetime.now().isoformat()}

    adquiridos = []
    vendidos = []
    _debug = []

    for ano in sorted(anuais.keys()):
        csvs = anuais[ano]
        for nome_csv, rows in csvs.items():
            if not rows:
                continue
            nl = nome_csv.lower()

            if "ativo_adquirido" in nl:
                tipo_transacao = "aquisicao"
            else:
                continue

            # ── Passo 1: achar a coluna CNPJ ──
            colunas = list(rows[0].keys())
            col_cnpj = None
            for c in colunas:
                c_normalizado = c.encode("ascii", "ignore").decode("ascii").strip().lower()
                if "cnpj" in c_normalizado:
                    col_cnpj = c
                    break

            # ── Passo 2: filtrar ──
            filtrado = []
            if col_cnpj:
                for row in rows:
                    v = re.sub(r"\D", "", str(row.get(col_cnpj, "") or ""))
                    if v == cnpj_limpo:
                        filtrado.append(row)

            _debug.append({
                "csv": nome_csv, "ano": ano,
                "total_rows": len(rows),
                "col_cnpj": col_cnpj,
                "col_cnpj_repr": repr(col_cnpj),
                "primeira_col_repr": repr(colunas[0]) if colunas else None,
                "filtrado": len(filtrado),
            })

            # ── Passo 3: montar registros ──
            for row in filtrado:
                dt = row.get("Data_Referencia") or row.get("DT_COMPTC") or ""
                registro = {
                    "data_referencia": dt,
                    "_ano": ano,
                    "_tipo_transacao": tipo_transacao,
                    "_csv": nome_csv,
                }
                for k, v in row.items():
                    if k.startswith("CNPJ") or k == "Data_Referencia" or k.startswith("DT_"):
                        continue
                    val = _extrair_float_safe(v)
                    if val != 0.0:
                        registro[k] = val
                    elif v and str(v).strip() not in ("0", "0.00", "None", "", "0,00"):
                        registro[k] = str(v).strip()

                if len(registro) > 4:
                    # Extrair melhor valor
                    valor = 0
                    for campo in ["Montante_Investido", "Valor_Aquisicao", "Custo_Aquisicao",
                                  "Valor_Alienacao", "Valor_Venda", "Valor_Total"]:
                        vv = registro.get(campo)
                        if vv and isinstance(vv, (int, float)) and vv > 0:
                            valor = vv
                            break
                    if not valor:
                        for k2, v2 in registro.items():
                            if isinstance(v2, (int, float)) and v2 > 1000 and not k2.startswith("_"):
                                kl = k2.lower()
                                if "valor" in kl or "montante" in kl or "custo" in kl:
                                    valor = v2
                                    break

                    registro["_valor"] = valor
                    registro["_direcao"] = "compra" if tipo_transacao == "aquisicao" else "venda"

                    if tipo_transacao == "aquisicao":
                        adquiridos.append(registro)
                    else:
                        vendidos.append(registro)

    # ── Parte 2: Alienações do Trimestral ──
    try:
        trimestrais = _download_trimestral_parallel(ano_inicio, ano_fim)
    except Exception:
        trimestrais = {}

    for ano in sorted(trimestrais.keys()):
        csvs = trimestrais[ano]
        for nome_csv, rows in csvs.items():
            if not rows:
                continue
            nl = nome_csv.lower()
            if "alienacao" not in nl:
                continue

            colunas = list(rows[0].keys())
            col_cnpj = None
            for c in colunas:
                c_norm = c.encode("ascii", "ignore").decode("ascii").strip().lower()
                if "cnpj" in c_norm:
                    col_cnpj = c
                    break

            filtrado = []
            if col_cnpj:
                for row in rows:
                    v = re.sub(r"\D", "", str(row.get(col_cnpj, "") or ""))
                    if v == cnpj_limpo:
                        filtrado.append(row)

            for row in filtrado:
                dt = row.get("Data_Referencia") or row.get("DT_COMPTC") or ""
                registro = {
                    "data_referencia": dt,
                    "_ano": ano,
                    "_tipo_transacao": "alienacao",
                    "_fonte": "trimestral",
                    "_csv": nome_csv,
                }
                for k, v in row.items():
                    if k.startswith("CNPJ") or k == "Data_Referencia" or k.startswith("DT_"):
                        continue
                    val = _extrair_float_safe(v)
                    if val != 0.0:
                        registro[k] = val
                    elif v and str(v).strip() not in ("0", "0.00", "None", "", "0,00"):
                        registro[k] = str(v).strip()

                if len(registro) > 4:
                    valor = 0
                    for campo in ["Valor_Alienacao", "Valor_Venda", "Montante_Investido", "Valor_Total"]:
                        vv = registro.get(campo)
                        if vv and isinstance(vv, (int, float)) and vv > 0:
                            valor = vv
                            break
                    registro["_valor"] = valor
                    registro["_direcao"] = "venda"
                    vendidos.append(registro)

    # Ordenar
    adquiridos.sort(key=lambda x: x.get("data_referencia", ""))
    vendidos.sort(key=lambda x: x.get("data_referencia", ""))
    timeline = sorted(adquiridos + vendidos,
                      key=lambda x: x.get("data_referencia", ""), reverse=True)

    total_compras = sum(t.get("_valor", 0) for t in adquiridos)
    total_vendas = sum(t.get("_valor", 0) for t in vendidos)

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "periodo": f"{ano_inicio}-{ano_fim}",
        "timeline": timeline,
        "adquiridos": adquiridos,
        "vendidos": vendidos,
        "resumo": {
            "total_transacoes": len(timeline),
            "total_aquisicoes": len(adquiridos),
            "total_alienacoes": len(vendidos),
            "volume_compras": total_compras,
            "volume_vendas": total_vendas,
            "saldo_liquido": total_compras - total_vendas,
        },
        "_debug": _debug,
        "consultado_em": datetime.now().isoformat(),
    }


CVM_TRIMESTRAL_CACHE = {}
CVM_ANUAL_CACHE = {}


def _baixar_csv_cvm_fii_trimestral_explore(ano: int) -> dict:
    """Baixa o ZIP do informe trimestral de FIIs da CVM."""
    cache_key = f"cvm_trim_explore:{ano}"
    cached = CVM_TRIMESTRAL_CACHE.get(cache_key)
    if cached:
        ts, data = cached
        if time.time() - ts < CVM_CSV_CACHE_TTL:
            return data

    url = f"https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/inf_trimestral_fii_{ano}.zip"

    try:
        resp = requests.get(url, timeout=90, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        resp.raise_for_status()
    except Exception as e:
        return {"erro": f"Erro ao baixar ZIP trimestral ({url}): {type(e).__name__}: {str(e)}"}

    if len(resp.content) < 100:
        return {"erro": f"ZIP muito pequeno ({len(resp.content)} bytes)"}

    resultado = {}
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for nome_arq in zf.namelist():
                if not nome_arq.endswith(".csv"):
                    continue
                with zf.open(nome_arq) as f:
                    raw = f.read()
                    try:
                        texto = raw.decode("utf-8-sig")
                    except UnicodeDecodeError:
                        texto = raw.decode("latin-1", errors="replace")
                    reader = csv.DictReader(io.StringIO(texto), delimiter=";")
                    rows = list(reader)
                    resultado[nome_arq] = rows
    except Exception as e:
        return {"erro": f"Erro ao processar ZIP: {type(e).__name__}: {str(e)}"}

    if resultado:
        CVM_TRIMESTRAL_CACHE[cache_key] = (time.time(), resultado)
    return resultado


def _baixar_csv_cvm_fii_anual(ano: int) -> dict:
    """Baixa o ZIP do informe anual de FIIs da CVM."""
    cache_key = f"cvm_anual:{ano}"
    cached = CVM_ANUAL_CACHE.get(cache_key)
    if cached:
        ts, data = cached
        if time.time() - ts < CVM_CSV_CACHE_TTL:
            return data

    url = f"https://dados.cvm.gov.br/dados/FII/DOC/INF_ANUAL/DADOS/inf_anual_fii_{ano}.zip"

    try:
        resp = requests.get(url, timeout=90, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        resp.raise_for_status()
    except Exception as e:
        return {"erro": f"Erro ao baixar ZIP anual ({url}): {type(e).__name__}: {str(e)}"}

    if len(resp.content) < 100:
        return {"erro": f"ZIP muito pequeno ({len(resp.content)} bytes)"}

    resultado = {}
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for nome_arq in zf.namelist():
                if not nome_arq.endswith(".csv"):
                    continue
                with zf.open(nome_arq) as f:
                    raw = f.read()
                    try:
                        texto = raw.decode("utf-8-sig")
                    except UnicodeDecodeError:
                        texto = raw.decode("latin-1", errors="replace")
                    reader = csv.DictReader(io.StringIO(texto), delimiter=";")
                    rows = list(reader)
                    resultado[nome_arq] = rows
    except Exception as e:
        return {"erro": f"Erro ao processar ZIP: {type(e).__name__}: {str(e)}"}

    if resultado:
        CVM_ANUAL_CACHE[cache_key] = (time.time(), resultado)
    return resultado


@app.get("/api/cvm/explorar-trimestral/{ticker}")
async def explorar_trimestral(
    ticker: str,
    ano: int = Query(default=2025, description="Ano do informe trimestral"),
):
    """
    Exploração: mostra TODOS os CSVs e colunas do Informe Trimestral CVM de um FII.
    
    O informe trimestral contém dados operacionais detalhados:
    - Receitas por imóvel / ativo
    - Contratos de aluguel (valor, prazo, reajuste)
    - Vacância física e financeira
    - Inadimplência
    - Encargos e despesas detalhados
    - Passivos discriminados
    
    Este endpoint baixa o ZIP, filtra pelo CNPJ do ticker, e mostra
    todos os dados disponíveis para mapeamento futuro.
    """
    ticker = ticker.upper().strip()
    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"erro": "CNPJ não encontrado", "ticker": ticker}

    csvs = _baixar_csv_cvm_fii_trimestral_explore(ano)
    if "erro" in csvs:
        return {"ticker": ticker, "ano": ano, **csvs}

    # Descobrir CNPJ_Fundo_Classe (Resolução CVM 175)
    cnpjs_classe = _descobrir_cnpj_classe({ano: csvs}, cnpj, ticker)

    resultado = {}
    total_registros_ticker = 0

    for nome_csv, rows in sorted(csvs.items()):
        if not rows:
            resultado[nome_csv] = {
                "total_registros_geral": 0,
                "total_registros_ticker": 0,
                "colunas": [],
            }
            continue

        colunas = list(rows[0].keys()) if rows else []
        filtrado = _filtrar_por_cnpj(rows, cnpj, cnpjs_classe)
        total_registros_ticker += len(filtrado)

        # Amostra: primeiros 3 registros do ticker (ou geral se não encontrar)
        amostras = filtrado[:3] if filtrado else rows[:1]

        # Identificar colunas com valores não-vazios no ticker
        colunas_com_dados = []
        if filtrado:
            for col in colunas:
                valores_unicos = set()
                for row in filtrado[:10]:
                    v = row.get(col, "")
                    if v and v not in ("0", "0.00", "None", "", "0,00"):
                        valores_unicos.add(v)
                if valores_unicos:
                    colunas_com_dados.append({
                        "coluna": col,
                        "exemplos": list(valores_unicos)[:5],
                        "total_preenchido": sum(1 for r in filtrado if r.get(col) and r.get(col) not in ("0", "0.00", "None", "", "0,00")),
                    })

        resultado[nome_csv] = {
            "total_registros_geral": len(rows),
            "total_registros_ticker": len(filtrado),
            "total_colunas": len(colunas),
            "colunas": colunas,
            "colunas_com_dados_ticker": colunas_com_dados,
            "amostras": amostras,
        }

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "ano": ano,
        "tipo_informe": "TRIMESTRAL",
        "url_fonte": f"https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/inf_trimestral_fii_{ano}.zip",
        "total_csvs": len(resultado),
        "total_registros_ticker": total_registros_ticker,
        "csvs": resultado,
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/explorar-anual/{ticker}")
async def explorar_anual(
    ticker: str,
    ano: int = Query(default=2024, description="Ano do informe anual"),
):
    """
    Exploração: mostra TODOS os CSVs e colunas do Informe Anual CVM de um FII.
    
    O informe anual contém:
    - Relatório do administrador
    - Política de investimento e desinvestimento
    - Relação de ativos do fundo (imóveis, CRIs, etc.)
    - Dados de governança
    - Remuneração do administrador/gestor
    
    Este endpoint baixa o ZIP, filtra pelo CNPJ do ticker, e mostra
    todos os dados disponíveis para mapeamento futuro.
    """
    ticker = ticker.upper().strip()
    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"erro": "CNPJ não encontrado", "ticker": ticker}

    csvs = _baixar_csv_cvm_fii_anual(ano)
    if "erro" in csvs:
        return {"ticker": ticker, "ano": ano, **csvs}

    # Descobrir CNPJ_Fundo_Classe (Resolução CVM 175)
    cnpjs_classe = _descobrir_cnpj_classe({ano: csvs}, cnpj, ticker)

    resultado = {}
    total_registros_ticker = 0

    for nome_csv, rows in sorted(csvs.items()):
        if not rows:
            resultado[nome_csv] = {
                "total_registros_geral": 0,
                "total_registros_ticker": 0,
                "colunas": [],
            }
            continue

        colunas = list(rows[0].keys()) if rows else []
        filtrado = _filtrar_por_cnpj(rows, cnpj, cnpjs_classe)
        total_registros_ticker += len(filtrado)

        amostras = filtrado[:3] if filtrado else rows[:1]

        # Identificar colunas com valores não-vazios no ticker
        colunas_com_dados = []
        if filtrado:
            for col in colunas:
                valores_unicos = set()
                for row in filtrado[:10]:
                    v = row.get(col, "")
                    if v and v not in ("0", "0.00", "None", "", "0,00"):
                        valores_unicos.add(v)
                if valores_unicos:
                    colunas_com_dados.append({
                        "coluna": col,
                        "exemplos": list(valores_unicos)[:5],
                        "total_preenchido": sum(1 for r in filtrado if r.get(col) and r.get(col) not in ("0", "0.00", "None", "", "0,00")),
                    })

        resultado[nome_csv] = {
            "total_registros_geral": len(rows),
            "total_registros_ticker": len(filtrado),
            "total_colunas": len(colunas),
            "colunas": colunas,
            "colunas_com_dados_ticker": colunas_com_dados,
            "amostras": amostras,
        }

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "nome": dados_fii.get("razao_social", ""),
        "ano": ano,
        "tipo_informe": "ANUAL",
        "url_fonte": f"https://dados.cvm.gov.br/dados/FII/DOC/INF_ANUAL/DADOS/inf_anual_fii_{ano}.zip",
        "total_csvs": len(resultado),
        "total_registros_ticker": total_registros_ticker,
        "csvs": resultado,
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/explorar-trimestral-colunas/{ano}")
async def explorar_trimestral_colunas(ano: int = 2025):
    """
    Exploração rápida: lista apenas os CSVs e suas colunas do informe trimestral,
    sem filtrar por ticker. Útil para entender a estrutura geral.
    """
    csvs = _baixar_csv_cvm_fii_trimestral_explore(ano)
    if "erro" in csvs:
        return {"ano": ano, **csvs}

    resultado = {}
    for nome_csv, rows in sorted(csvs.items()):
        colunas = list(rows[0].keys()) if rows else []
        # Pegar 1 amostra genérica
        amostra = rows[0] if rows else {}
        resultado[nome_csv] = {
            "total_registros": len(rows),
            "total_colunas": len(colunas),
            "colunas": colunas,
            "amostra": amostra,
        }

    return {
        "ano": ano,
        "tipo_informe": "TRIMESTRAL",
        "total_csvs": len(resultado),
        "csvs": resultado,
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/explorar-anual-colunas/{ano}")
async def explorar_anual_colunas(ano: int = 2024):
    """
    Exploração rápida: lista apenas os CSVs e suas colunas do informe anual,
    sem filtrar por ticker.
    """
    csvs = _baixar_csv_cvm_fii_anual(ano)
    if "erro" in csvs:
        return {"ano": ano, **csvs}

    resultado = {}
    for nome_csv, rows in sorted(csvs.items()):
        colunas = list(rows[0].keys()) if rows else []
        amostra = rows[0] if rows else {}
        resultado[nome_csv] = {
            "total_registros": len(rows),
            "total_colunas": len(colunas),
            "colunas": colunas,
            "amostra": amostra,
        }

    return {
        "ano": ano,
        "tipo_informe": "ANUAL",
        "total_csvs": len(resultado),
        "csvs": resultado,
        "consultado_em": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# Debug: Listar colunas dos CSVs do informe mensal
# ─────────────────────────────────────────────────────────────

@app.get("/api/cvm/debug-colunas-mensal/{ano}")
async def debug_colunas_mensal(ano: int = 2025):
    """
    Debug: lista TODAS as colunas de cada CSV do informe mensal FII.
    Útil para descobrir campos de amortização e outros.
    """
    csvs = _baixar_csv_cvm_fii(ano)
    if "erro" in csvs:
        return csvs
    
    resultado = {}
    for nome_csv, rows in csvs.items():
        if not rows:
            resultado[nome_csv] = {"colunas": [], "total_registros": 0}
            continue
        
        colunas = list(rows[0].keys()) if rows else []
        
        # Procurar colunas relevantes
        colunas_amort = [c for c in colunas if "amort" in c.lower()]
        colunas_rendim = [c for c in colunas if "rendim" in c.lower() or "distrib" in c.lower()]
        colunas_cota = [c for c in colunas if "cota" in c.lower() or "COTA" in c]
        
        # Amostra: primeiro registro com valores não-nulos
        amostra = {}
        for row in rows[:5]:
            for k, v in row.items():
                if v and v not in ("0", "0.00", "None", ""):
                    if k not in amostra:
                        amostra[k] = v
        
        resultado[nome_csv] = {
            "total_colunas": len(colunas),
            "colunas": colunas,
            "colunas_amortizacao": colunas_amort,
            "colunas_rendimentos": colunas_rendim,
            "colunas_cotas": colunas_cota,
            "total_registros": len(rows),
            "amostra_valores": amostra,
        }
    
    return {
        "ano": ano,
        "csvs": resultado,
        "consultado_em": datetime.now().isoformat(),
    }


@app.get("/api/cvm/debug-colunas-mensal-ticker/{ticker}")
async def debug_colunas_mensal_ticker(
    ticker: str,
    ano: int = Query(default=2024, description="Ano para inspecionar"),
):
    """
    Debug: mostra TODOS os campos e valores de um ticker específico
    em TODOS os CSVs do informe mensal. Útil para encontrar amortização.
    """
    ticker = ticker.upper().strip()
    dados_fii = descobrir_dados_fii(ticker)
    cnpj = dados_fii.get("cnpj", "")
    if not cnpj:
        return {"erro": "CNPJ não encontrado"}

    csvs = _baixar_csv_cvm_fii(ano)
    if "erro" in csvs:
        return csvs

    resultado = {}
    for nome_csv, rows in csvs.items():
        filtrado = _filtrar_por_cnpj(rows, cnpj)
        if not filtrado:
            continue
        
        colunas = list(filtrado[0].keys())
        colunas_amort = [c for c in colunas if "amort" in c.lower()]
        
        # Pegar primeiros 3 registros completos
        amostras = filtrado[:3]
        
        resultado[nome_csv] = {
            "colunas": colunas,
            "colunas_amortizacao": colunas_amort,
            "total_registros": len(filtrado),
            "registros": amostras,
        }

    return {
        "ticker": ticker,
        "cnpj": cnpj,
        "ano": ano,
        "csvs": resultado,
        "consultado_em": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
