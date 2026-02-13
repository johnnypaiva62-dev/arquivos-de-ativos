"""
Brasil Asset Research — API REST
=================================
Envolve o br_asset_research.py com FastAPI para uso via frontend (Lovable, etc.).

Endpoints:
  GET  /api/health                    → Healthcheck
  GET  /api/buscar/{ticker}           → Lista documentos FNET + indicadores
  GET  /api/fnet/{ticker}             → Lista só documentos FNET
  GET  /api/fnet/download/{doc_id}    → Proxy de download do PDF
  GET  /api/indicadores/{ticker}      → Indicadores de sites de mercado

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
from fastapi.responses import StreamingResponse, JSONResponse

# ─────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Brasil Asset Research API",
    version="1.0.0",
    description="API para pesquisa de FIIs e Ações brasileiras — FNET, CVM, indicadores",
)

# CORS — permitir chamadas do Lovable e localhost
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "https://*.lovable.app",
        "https://*.lovableproject.com",
        "*",  # Em produção, restrinja aos seus domínios
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# Constantes e helpers (extraídos do br_asset_research.py)
# ─────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

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

session = requests.Session()
session.headers.update(HEADERS)


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


def descobrir_cnpj_fii(ticker: str) -> Optional[str]:
    """Busca CNPJ de um FII no cadastro da CVM."""
    url = "https://dados.cvm.gov.br/dados/FII/CAD/DADOS/cad_fii.csv"
    resp = safe_get(url, timeout=30)
    if not resp:
        return None
    for linha in resp.text.split("\n"):
        if ticker in linha.upper():
            match = re.search(r"(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", linha)
            if match:
                return match.group(1)
    return None


# Cache simples em memória
_cache: dict = {}
CACHE_TTL = 600  # 10 minutos


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
# Endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/tipo/{ticker}")
async def tipo_ativo(ticker: str):
    """Detecta se o ticker é FII ou Ação."""
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    return {"ticker": ticker, "tipo": tipo, "label": "FII" if tipo == "fii" else "Ação"}


@app.get("/api/fnet/{ticker}")
async def listar_fnet(
    ticker: str,
    max_docs: int = Query(default=20, ge=1, le=100),
    categoria: Optional[str] = Query(default=None, description="Filtrar por categoria"),
):
    """
    Lista documentos do FNET para um FII.
    Retorna lista de documentos com links de download.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)
    if tipo != "fii":
        raise HTTPException(400, f"{ticker} não parece ser um FII. Use /api/indicadores para ações.")

    cache_key = f"fnet:{ticker}:{max_docs}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    # Descobrir CNPJ
    cnpj = descobrir_cnpj_fii(ticker)

    # Buscar no FNET
    url = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
    payload = {
        "d": "0",
        "s": "0",
        "l": str(max_docs),
        "o[0][dataEntrega]": "desc",
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

    resp = safe_post(url, data=payload)
    if not resp:
        raise HTTPException(502, "FNET indisponível no momento. Tente novamente.")

    try:
        data = resp.json()
    except json.JSONDecodeError:
        raise HTTPException(502, "Resposta inválida do FNET.")

    docs = []
    for item in data.get("data", [])[:max_docs]:
        doc_id = item.get("id")
        doc = {
            "id": doc_id,
            "categoria": item.get("descricaoCategoria", ""),
            "tipo": item.get("descricaoTipo", ""),
            "data_entrega": item.get("dataEntrega", ""),
            "data_referencia": item.get("dataReferencia", ""),
            "status": item.get("situacao", ""),
            "url_download": f"/api/fnet/download/{doc_id}",
            "url_fnet": f"https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={doc_id}",
        }

        # Filtrar por categoria se solicitado
        if categoria and categoria.lower() not in doc["categoria"].lower():
            continue

        docs.append(doc)

    result = {
        "ticker": ticker,
        "cnpj": cnpj,
        "total_fnet": data.get("recordsTotal", len(docs)),
        "listados": len(docs),
        "documentos": docs,
        "consultado_em": datetime.now().isoformat(),
    }

    cache_set(cache_key, result)
    return result


@app.get("/api/fnet/download/{doc_id}")
async def download_fnet_pdf(doc_id: int):
    """
    Proxy de download do PDF do FNET.
    Resolve problemas de CORS — o navegador baixa via esta API, não direto do FNET.
    """
    url = f"https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id={doc_id}"

    try:
        resp = session.get(url, timeout=120, stream=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(502, f"Erro ao baixar do FNET: {str(e)}")

    # Extrair nome do arquivo do header
    cd = resp.headers.get("Content-Disposition", "")
    match = re.findall(r'filename="?([^";\n]+)', cd)
    filename = match[0] if match else f"fnet_{doc_id}.pdf"

    return StreamingResponse(
        io.BytesIO(resp.content),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(resp.content)),
        },
    )


@app.get("/api/indicadores/{ticker}")
async def indicadores(ticker: str):
    """
    Busca indicadores básicos de sites de mercado.
    Funciona para FIIs e Ações.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    cache_key = f"indicadores:{ticker}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    dados = {
        "ticker": ticker,
        "tipo": tipo,
        "label": "FII" if tipo == "fii" else "Ação",
        "indicadores": {},
        "proventos": None,
        "consultado_em": datetime.now().isoformat(),
    }

    # Status Invest
    tipo_url = "fundos-imobiliarios" if tipo == "fii" else "acoes"
    si_url = f"https://statusinvest.com.br/{tipo_url}/{ticker.lower()}"
    resp = safe_get(si_url)
    if resp:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        for s in soup.find_all("strong", class_="value"):
            parent = s.find_parent("div")
            if parent:
                label_el = parent.find("h3") or parent.find("span", class_="sub-value")
                if label_el:
                    key = label_el.get_text(strip=True)
                    dados["indicadores"][key] = s.get_text(strip=True)

        # Proventos API
        prov_url = (
            f"https://statusinvest.com.br/fii/tickerprovents?ticker={ticker}&chartProv498=true"
            if tipo == "fii"
            else f"https://statusinvest.com.br/acao/companytickerprovents?ticker={ticker}&chartProv498=true"
        )
        resp2 = safe_get(prov_url, timeout=15)
        if resp2:
            try:
                dados["proventos"] = resp2.json()
            except json.JSONDecodeError:
                pass

    result = dados
    cache_set(cache_key, result)
    return result


@app.get("/api/buscar/{ticker}")
async def busca_completa(
    ticker: str,
    max_docs: int = Query(default=20, ge=1, le=100),
):
    """
    Pesquisa completa: FNET (se FII) + indicadores.
    Endpoint principal para o dashboard.
    """
    ticker = ticker.upper().strip()
    tipo = detectar_tipo_ativo(ticker)

    resultado = {
        "ticker": ticker,
        "tipo": tipo,
        "label": "FII" if tipo == "fii" else "Ação",
        "fnet": None,
        "indicadores": None,
        "consultado_em": datetime.now().isoformat(),
    }

    # FNET (só para FIIs)
    if tipo == "fii":
        try:
            fnet_data = await listar_fnet(ticker, max_docs=max_docs)
            resultado["fnet"] = fnet_data
        except HTTPException:
            resultado["fnet"] = {"erro": "FNET indisponível"}

    # Indicadores (para todos)
    try:
        ind_data = await indicadores(ticker)
        resultado["indicadores"] = ind_data
    except HTTPException:
        resultado["indicadores"] = {"erro": "Indicadores indisponíveis"}

    return resultado


# ─────────────────────────────────────────────────────────────
# Executar com: uvicorn main:app --host 0.0.0.0 --port 8000
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
