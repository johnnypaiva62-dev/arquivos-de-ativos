// ============================================================
// FnetDocumentBrowser.tsx
// ============================================================
// Componente React/TypeScript para buscar e baixar relatórios
// do FNET direto no seu dashboard Lovable.
//
// COMO USAR NO LOVABLE:
// 1. Crie um novo componente e cole este código
// 2. Altere API_BASE_URL para a URL da sua API deployada
// 3. Importe e use: <FnetDocumentBrowser />
// ============================================================

import { useState } from "react";

// ╔═══════════════════════════════════════════════════════╗
// ║  ALTERE ESTA URL PARA A DO SEU DEPLOY               ║
// ╚═══════════════════════════════════════════════════════╝
const API_BASE_URL = "https://SUA-API.railway.app";
// Exemplos:
//   Railway:  "https://brasil-asset-api-production.up.railway.app"
//   Render:   "https://brasil-asset-api.onrender.com"
//   Local:    "http://localhost:8000"

// ─────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────

interface FnetDocument {
  id: number;
  categoria: string;
  tipo: string;
  data_entrega: string;
  data_referencia: string;
  url_download: string;
  url_fnet: string;
}

interface FnetResponse {
  ticker: string;
  cnpj: string | null;
  total_fnet: number;
  listados: number;
  documentos: FnetDocument[];
  consultado_em: string;
}

// ─────────────────────────────────────────────────────────
// Componente principal
// ─────────────────────────────────────────────────────────

export default function FnetDocumentBrowser() {
  const [ticker, setTicker] = useState("");
  const [maxDocs, setMaxDocs] = useState(20);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<FnetResponse | null>(null);
  const [downloading, setDownloading] = useState<number | null>(null);
  const [filtroCategoria, setFiltroCategoria] = useState("todas");

  // Buscar documentos
  const buscarDocumentos = async () => {
    const t = ticker.trim().toUpperCase();
    if (!t) return;

    setLoading(true);
    setError(null);
    setData(null);

    try {
      const resp = await fetch(
        `${API_BASE_URL}/api/fnet/${t}?max_docs=${maxDocs}`
      );

      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(
          errData.detail || `Erro ${resp.status}: ${resp.statusText}`
        );
      }

      const json: FnetResponse = await resp.json();
      setData(json);
    } catch (err: any) {
      setError(err.message || "Erro ao buscar documentos");
    } finally {
      setLoading(false);
    }
  };

  // Download de PDF via proxy da API
  const baixarPdf = async (doc: FnetDocument) => {
    setDownloading(doc.id);
    try {
      const resp = await fetch(`${API_BASE_URL}${doc.url_download}`);
      if (!resp.ok) throw new Error("Falha no download");

      const blob = await resp.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${data?.ticker || "doc"}_fnet_${doc.id}.pdf`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
    } catch (err: any) {
      alert(`Erro ao baixar: ${err.message}`);
    } finally {
      setDownloading(null);
    }
  };

  // Abrir direto no FNET (nova aba)
  const abrirNoFnet = (doc: FnetDocument) => {
    window.open(doc.url_fnet, "_blank", "noopener");
  };

  // Extrair categorias únicas para filtro
  const categorias = data
    ? [...new Set(data.documentos.map((d) => d.categoria))].sort()
    : [];

  const docsFiltrados = data
    ? filtroCategoria === "todas"
      ? data.documentos
      : data.documentos.filter((d) => d.categoria === filtroCategoria)
    : [];

  // ─────────────────────────────────────────────────────
  // Render
  // ─────────────────────────────────────────────────────

  return (
    <div style={{ fontFamily: "Inter, system-ui, sans-serif", maxWidth: 900, margin: "0 auto" }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ margin: 0, fontSize: 20, fontWeight: 600 }}>
          📄 Relatórios FNET
        </h2>
        <p style={{ margin: "4px 0 0", color: "#666", fontSize: 14 }}>
          Busque e baixe relatórios gerenciais de FIIs direto da B3
        </p>
      </div>

      {/* Barra de busca */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
        <input
          type="text"
          placeholder="Ticker (ex: BLCA11)"
          value={ticker}
          onChange={(e) => setTicker(e.target.value.toUpperCase())}
          onKeyDown={(e) => e.key === "Enter" && buscarDocumentos()}
          style={{
            padding: "8px 12px",
            border: "1px solid #ddd",
            borderRadius: 6,
            fontSize: 14,
            width: 160,
            textTransform: "uppercase",
          }}
        />
        <select
          value={maxDocs}
          onChange={(e) => setMaxDocs(Number(e.target.value))}
          style={{
            padding: "8px 12px",
            border: "1px solid #ddd",
            borderRadius: 6,
            fontSize: 14,
          }}
        >
          <option value={10}>10 docs</option>
          <option value={20}>20 docs</option>
          <option value={50}>50 docs</option>
          <option value={100}>100 docs</option>
        </select>
        <button
          onClick={buscarDocumentos}
          disabled={loading || !ticker.trim()}
          style={{
            padding: "8px 20px",
            background: loading ? "#ccc" : "#2563eb",
            color: "#fff",
            border: "none",
            borderRadius: 6,
            fontSize: 14,
            fontWeight: 500,
            cursor: loading ? "wait" : "pointer",
          }}
        >
          {loading ? "Buscando..." : "Buscar"}
        </button>
      </div>

      {/* Erro */}
      {error && (
        <div
          style={{
            padding: "10px 14px",
            background: "#fef2f2",
            border: "1px solid #fecaca",
            borderRadius: 6,
            color: "#dc2626",
            fontSize: 14,
            marginBottom: 16,
          }}
        >
          ❌ {error}
        </div>
      )}

      {/* Resultados */}
      {data && (
        <>
          {/* Info do ticker */}
          <div
            style={{
              padding: "10px 14px",
              background: "#f0f9ff",
              border: "1px solid #bae6fd",
              borderRadius: 6,
              fontSize: 14,
              marginBottom: 16,
              display: "flex",
              justifyContent: "space-between",
              flexWrap: "wrap",
              gap: 8,
            }}
          >
            <span>
              <strong>{data.ticker}</strong>
              {data.cnpj && <span style={{ color: "#666" }}> — CNPJ: {data.cnpj}</span>}
            </span>
            <span style={{ color: "#666" }}>
              {data.listados} de {data.total_fnet} documentos
            </span>
          </div>

          {/* Filtro por categoria */}
          {categorias.length > 1 && (
            <div style={{ marginBottom: 12 }}>
              <select
                value={filtroCategoria}
                onChange={(e) => setFiltroCategoria(e.target.value)}
                style={{
                  padding: "6px 10px",
                  border: "1px solid #ddd",
                  borderRadius: 6,
                  fontSize: 13,
                }}
              >
                <option value="todas">Todas as categorias ({data.documentos.length})</option>
                {categorias.map((cat) => (
                  <option key={cat} value={cat}>
                    {cat} ({data.documentos.filter((d) => d.categoria === cat).length})
                  </option>
                ))}
              </select>
            </div>
          )}

          {/* Tabela de documentos */}
          <div style={{ overflowX: "auto" }}>
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontSize: 13,
              }}
            >
              <thead>
                <tr style={{ borderBottom: "2px solid #e5e7eb" }}>
                  <th style={thStyle}>Data</th>
                  <th style={thStyle}>Categoria</th>
                  <th style={thStyle}>Tipo</th>
                  <th style={thStyle}>Referência</th>
                  <th style={{ ...thStyle, textAlign: "center" }}>Ações</th>
                </tr>
              </thead>
              <tbody>
                {docsFiltrados.map((doc) => (
                  <tr
                    key={doc.id}
                    style={{ borderBottom: "1px solid #f3f4f6" }}
                  >
                    <td style={tdStyle}>{formatDate(doc.data_entrega)}</td>
                    <td style={tdStyle}>
                      <span style={badgeStyle(doc.categoria)}>
                        {doc.categoria}
                      </span>
                    </td>
                    <td style={tdStyle}>{doc.tipo}</td>
                    <td style={tdStyle}>{formatDate(doc.data_referencia)}</td>
                    <td style={{ ...tdStyle, textAlign: "center" }}>
                      <div style={{ display: "flex", gap: 6, justifyContent: "center" }}>
                        <button
                          onClick={() => baixarPdf(doc)}
                          disabled={downloading === doc.id}
                          style={btnSmallStyle("#2563eb")}
                          title="Baixar PDF"
                        >
                          {downloading === doc.id ? "⏳" : "⬇️"} PDF
                        </button>
                        <button
                          onClick={() => abrirNoFnet(doc)}
                          style={btnSmallStyle("#6b7280")}
                          title="Abrir no FNET"
                        >
                          🔗 FNET
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {docsFiltrados.length === 0 && (
            <p style={{ textAlign: "center", color: "#999", padding: 20 }}>
              Nenhum documento encontrado para este filtro.
            </p>
          )}
        </>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────
// Estilos auxiliares
// ─────────────────────────────────────────────────────────

const thStyle: React.CSSProperties = {
  textAlign: "left",
  padding: "8px 10px",
  fontSize: 12,
  fontWeight: 600,
  color: "#6b7280",
  textTransform: "uppercase",
  letterSpacing: "0.05em",
};

const tdStyle: React.CSSProperties = {
  padding: "10px",
  verticalAlign: "middle",
};

const btnSmallStyle = (bg: string): React.CSSProperties => ({
  padding: "4px 10px",
  background: bg,
  color: "#fff",
  border: "none",
  borderRadius: 4,
  fontSize: 12,
  cursor: "pointer",
  whiteSpace: "nowrap",
});

const badgeColors: Record<string, string> = {
  "Relatório Gerencial": "#059669",
  "Informe Mensal": "#2563eb",
  "Informe Trimestral": "#7c3aed",
  "Fato Relevante": "#dc2626",
  "Aviso aos Cotistas": "#d97706",
};

const badgeStyle = (categoria: string): React.CSSProperties => {
  const color = badgeColors[categoria] || "#6b7280";
  return {
    display: "inline-block",
    padding: "2px 8px",
    background: color + "15",
    color: color,
    borderRadius: 4,
    fontSize: 12,
    fontWeight: 500,
  };
};

function formatDate(dateStr: string): string {
  if (!dateStr) return "—";
  // Formato FNET: "dd/MM/yyyy HH:mm" ou ISO
  if (dateStr.includes("T")) {
    const d = new Date(dateStr);
    return d.toLocaleDateString("pt-BR");
  }
  // Já está em dd/MM/yyyy
  return dateStr.split(" ")[0];
}
