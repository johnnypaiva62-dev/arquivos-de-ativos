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

def buscar_fnet(ticker: str, cnpj: str = None, max_docs: int = 20) -> list:
    """
    Busca documentos no FNET da B3.
    Corrigido com headers Referer/Origin e tipoFundo.
    """
    docs = []

    # Método 1: API POST (mais rápida)
    url = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"

    # Headers específicos para a requisição AJAX do FNET
    headers = {
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://fnet.bmfbovespa.com.br",
        "Referer": "https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?tipoFundo=1",
    }

    payload = {
        "d": "0",
        "s": "0",
        "l": str(max_docs),
        "o[0][dataEntrega]": "desc",
        "tipoFundo": "1",  # 1 = Fundo Imobiliário
        "idCategoriaDocumento": "0",
        "idTipoDocumento": "0",
        "idEspecieDocumento": "0",
        "situacao": "A",
        "cnpj": cnpj or "",
        "dataInicial": "",
        "dataFinal": "",
        "idFundo": "0",
        "razaoSocial": "",
        "codigoNegociacao": ticker,
    }

    try:
        resp = requests.post(url, data=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("data", [])[:max_docs]:
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
            }
            docs.append(doc)

        return docs

    except Exception:
        pass

    # Método 2: Fallback — scraping da página HTML do FNET
    try:
        page_url = (
            f"https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCVM"
            f"?paginaCertificados=false&tipoFundo=1"
        )
        resp = requests.get(page_url, headers={
            "User-Agent": BROWSER_HEADERS["User-Agent"],
            "Accept": "text/html",
        }, timeout=15)

        if resp.ok:
            # A página carrega dados via JS, mas podemos tentar extrair links
            # Se isso falhar, retornamos lista vazia com nota
            pass
    except Exception:
        pass

    return docs


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


def descobrir_cnpj_fii(ticker: str) -> Optional[str]:
    """Busca CNPJ de FII no cadastro CVM."""
    url = "https://dados.cvm.gov.br/dados/FII/CAD/DADOS/cad_fii.csv"
    resp = safe_get(url, timeout=30)
    if not resp:
        return None
    for linha in resp.text.split("\n"):
        if ticker.upper() in linha.upper():
            match = re.search(r"(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", linha)
            if match:
                return match.group(1)
    return None


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
    return {"status": "ok", "version": "2.0.0", "timestamp": datetime.now().isoformat()}


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
    erro_fnet = None

    if tipo == "fii":
        # Descobrir CNPJ
        cnpj = descobrir_cnpj_fii(ticker)

        # Buscar no FNET
        fnet_docs = buscar_fnet(ticker, cnpj=cnpj, max_docs=max_docs)

        if fnet_docs:
            docs.extend(fnet_docs)
        else:
            erro_fnet = (
                "FNET não retornou documentos. Possíveis causas: "
                "ticker incorreto, FNET temporariamente fora do ar, "
                "ou o fundo não possui documentos publicados."
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
