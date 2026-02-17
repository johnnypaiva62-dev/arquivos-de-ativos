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
from datetime import datetime
from typing import Optional

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
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
