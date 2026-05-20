from __future__ import annotations

import io

import time

import zipfile

from dataclasses import replace

from datetime import datetime, timezone

import httpx

import colombia_forecasting_desk.fetchers as fetchers

import colombia_forecasting_desk.source_fetching.dane as dane_fetchers

import colombia_forecasting_desk.source_fetching.imprenta as imprenta_fetchers

import colombia_forecasting_desk.source_fetching.minhacienda as minhacienda_fetchers

import colombia_forecasting_desk.source_fetching.registraduria as registraduria_fetchers

from colombia_forecasting_desk.fetchers import (
    SOCRATA_ADAPTERS,
    SocrataAdapter,
    _enrich_dane_icoced_xlsx,
    _enrich_banrep_minutas_html,
    _enrich_banrep_minutas_html_with_browser,
    _enrich_diario_oficial_pdfs,
    _enrich_gaceta_pdfs,
    _enrich_mincit_zonas_francas,
    _enrich_pdf_text,
    _enrich_senado_agenda_pdfs,
    _annotate_legal_identity_items,
    _extract_anchors,
    _extract_banrep_minutas_metadata,
    _extract_corte_comunicados,
    _extract_dane_comunicados,
    _extract_dian_regulatory_project_links,
    _extract_eltiempo_colombia_section,
    _extract_imprenta_jsf_table,
    _extract_mincit_zonas_francas_approved_rows_from_text,
    _extract_minhacienda_decree_projects,
    _extract_minhacienda_decree_projects_from_reader_markdown,
    _extract_minhacienda_tes_auction_facts,
    _extract_minhacienda_tes_auction_rows_from_text,
    _extract_pdf_text,
    _extract_registraduria_news_article_detail,
    _extract_registraduria_news_cards,
    _extract_senado_agenda_entries_from_text,
    _cap_items,
    _parse_diario_oficial_pdf_text,
    _parse_gaceta_pdf_text,
    _parse_rss_entries,
    _parse_dane_icoced_xlsx,
    _parse_date_text_to_iso,
    _parse_socrata_date,
    _recover_rss_entries,
    _socrata_params,
    _socrata_row_to_item,
    _struct_time_to_iso,
    _fetch_dian_regulatory_projects_api,
    fetch_api,
    fetch_html,
    fetch_rss,
)

from colombia_forecasting_desk.models import RawItem

from colombia_forecasting_desk.observability import RunTrace

def _xlsx_cell(ref: str, value: str | float | int) -> str:
    if isinstance(value, str):
        return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'
    return f'<c r="{ref}"><v>{value}</v></c>'

def _xlsx_row(row_num: int, values: dict[str, str | float | int]) -> str:
    cells = "".join(
        _xlsx_cell(f"{col}{row_num}", value) for col, value in values.items()
    )
    return f'<row r="{row_num}">{cells}</row>'

def _minimal_icoced_xlsx() -> bytes:
    workbook = """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Anexo 1" sheetId="1" r:id="rId1"/>
    <sheet name="Anexo 2.1" sheetId="2" r:id="rId2"/>
    <sheet name="Anexo 2.2" sheetId="3" r:id="rId3"/>
  </sheets>
</workbook>"""
    rels = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet3.xml"/>
</Relationships>"""

    def sheet(row_values: dict[str, str | float | int]) -> str:
        rows = [
            _xlsx_row(1, {"A": 2026, "B": "Enero"}),
            _xlsx_row(2, row_values),
        ]
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f"<sheetData>{''.join(rows)}</sheetData></worksheet>"
        )

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", rels)
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            sheet({"B": "Marzo", "C": 135.44, "D": 0.75, "E": 6.47, "F": 6.33}),
        )
        zf.writestr(
            "xl/worksheets/sheet2.xml",
            sheet({"B": "Marzo", "C": 134.81, "D": 0.77, "E": 6.43, "F": 6.26}),
        )
        zf.writestr(
            "xl/worksheets/sheet3.xml",
            sheet({"B": "Marzo", "C": 136.63, "D": 0.72, "E": 6.53, "F": 6.47}),
        )
    return out.getvalue()

MINHACIENDA_TES_COP_TEXT = """
MINHACIENDA REALIZA SUBASTA DE TES COP POR
$6,0 BILLONES, LA MAYOR REALIZADA A LA FECHA
El Ministerio de Hacienda y Crédito Público Minhacienda emitió hoy $6,0
billones en la subasta de TES denominados en pesos (COP) con vencimiento a
cuatro, nueve, catorce y treinta y dos años.
Se recibieron órdenes de compra por $12,3 billones, 4,1 veces el monto
inicialmente ofrecido.
Las tasas de interés de corte de la subasta fueron de 14,790% para los
TES 2030, 14,300% para los TES 2035, 13,968% para los TES 2040 y
13,940% para los TES 2058.
Bogotá, 13 de mayo de 2026
Tabla 1
Resultados Subasta TES COP
Plazo al vencimiento 4 años 9 años 14 años 32 años
Fecha de Vencimiento 27-feb-30 24-ene-35 28-nov-40 13-mar-58
Tasa cupón 12.500% 11.750% 12.750% 12.000%
Tasa de corte 14.790% 14.300% 13.968% 13.940%
Ofertas Recibidas $5.4 billones $4.2 billones $740 mil millones $1.9 billones
Monto Aprobado $2.8 billones $2.2 billones $178 mil millones $890 mil millones
(Fin).
"""

IRC_TES_COP_TEXT = """
RESUMEN SUBASTA TES TASA FIJA
Subdirección Financiamiento Interno de la Nación
Dirección General de Crédito Público y Tesoro Nacional
Ministerio de Hacienda y Crédito Público
Subasta No. 9 13 de mayo de 2026
FECHA PLAZO AL VTO. TASA MÍNIMA TASA MÁXIMA TASA PROMEDIO TASA DE
CORTE TASA SEN PRECIA PRECIO CUPÓN "TI" TAIL PB
VENCIMIENTO (AÑOS)
27-feb-30 4 14,520% 15,100% 14,810% 14,790% 14,760% 14,546% 96,120 12,50% 14,785% 0,459
24-ene-35 9 14,099% 14,661% 14,380% 14,300% 14,260% 14,053% 91,087 11,75% 14,346% -4,572
28-nov-40 14 13,820% 14,411% 14,116% 13,968% 13,902% 13,768% 98,174 12,75% 14,098% -13,039
13-mar-58 32 13,650% 14,297% 13,974% 13,940% 13,850% 13,639% 88,200 12,00% 13,983% -4,332
MONTOS
SESION COMPETITIVA
FECHA MONTO OFERTADO MONTO OFERTADO MONTO APROBADO MONTO APROBADO
BID/COVER
VENCIMIENTO Valor Nominal Valor Costo Valor Nominal Valor Costo
27-feb-30 5.698.333.000.000 5.430.910.232.310 2.886.053.800.000 2.774.074.912.560
24-ene-35 4.677.500.000.000 4.187.531.875.000 2.369.000.000.000 2.157.851.030.000
28-nov-40 773.000.000.000 739.660.510.000 181.000.000.000 177.694.940.000
13-mar-58 2.228.500.000.000 1.917.802.530.000 1.009.500.000.000 890.379.000.000
TOTAL 13.377.333.000.000 12.275.905.147.310 6.445.553.800.000 5.999.999.882.560 4,1
"""

BANREP_MINUTAS_DETAIL_HTML = """
<html><body><main>
  <h1>Minutas BanRep: La Junta Directiva del Banco de la República decidió por
  mayoría incrementar en 100 puntos básicos (pbs) la tasa de interés de política
  monetaria a 11,25%</h1>
  <a href="/es/print/pdf/node/65818">View PDF</a>
  <h2>Adjuntos</h2>
  <a href="https://d1b4gd4m8561gs.cloudfront.net/sites/default/files/anexo.pdf">
    Anexo estadístico
  </a>
  <p>Cuatro directores votaron a favor de esta decisión, dos por una reducción
  de 50 pbs y uno por mantenerla inalterada.</p>
  <p>Fecha de publicación: Martes, 07 de abril de 2026 19:44</p>
  <ul>
    <li>La inflación total en enero y febrero se situó en 5,4% y 5,3%,
    respectivamente, por encima del nivel observado al cierre de 2025.</li>
    <li>Las expectativas de inflación total continúan elevadas y alejadas de la
    meta, aunque las encuestas a analistas mostraron ligeras reducciones.</li>
  </ul>
  <p>El grupo mayoritario que votó por incrementar la tasa de interés de
  política en 100 pbs recordó que la decisión de enero no era suficiente. Los
  miembros de este grupo subrayaron el comportamiento de la inflación total y
  básica.</p>
  <p>Los directores que votaron por una reducción de 50 pbs de la tasa de
  interés de política destacaron que la inflación observada responde más a
  choques de oferta.</p>
  <p>El miembro de la Junta que votó por mantener inalterada la tasa de interés
  de política señaló que el ciclo de crisis provocado por la pandemia aún no se
  estabiliza.</p>
  <p>Próximas reuniones, minutas, informes y presentaciones ABR 30 Reunión tasa
  de interés de intervención.</p>
</main></body></html>
"""

BANREP_MINUTAS_DRUPAL_DETAIL_HTML = """
<html><body>
  <div class="block-page-title-block">
    <h1>Minutas BanRep: La Junta Directiva del Banco de la República decidió por
    unanimidad mantener inalterada la tasa de interés de política monetaria en
    11,25%</h1>
  </div>
  <div data-history-node-id="65916" class="node node--type-noticias">
    <div class="field--name-field-file">
      <a href="//d1b4gd4m8561gs.cloudfront.net/sites/default/files/paginas/anexo-estadistico-abril-2026.pdf">
        Anexo estadístico
      </a>
    </div>
    <div class="field-label">Fecha de publicación:</div>
    Miércoles, 06 de mayo de 2026
    <div class="body field-node--body">
      <p>La Junta Directiva tuvo en cuenta los siguientes elementos:</p>
      <ul>
        <li>En marzo la inflación total se situó en 5,6% superando en 46 pbs el
        dato de diciembre.</li>
        <li>El mercado laboral continúa dinámico, con niveles de desempleo
        históricamente bajos y tendencias crecientes en el empleo asalariado.</li>
      </ul>
      <p>La decisión adoptada por unanimidad de mantener inalterada la tasa de
      interés de política envía un mensaje de consenso entre los miembros de la
      Junta Directiva.</p>
      <p>Un grupo de cuatro directores manifestó su preocupación por el
      incremento que se ha venido observando en la inflación total y básica, y
      en sus expectativas. Subrayaron la persistencia inflacionaria.</p>
      <p>Los dos directores que abogan por una postura de política monetaria más
      relajada sostienen que la inflación anual ha descendido sustancialmente y
      que sus incrementos recientes obedecen a choques de oferta.</p>
      <p>Otro miembro de la Junta analiza que la inflación en marzo estuvo
      explicada por diversos factores entre los que predominan los choques de
      oferta.</p>
      <p>Asimismo, resaltaron que, en la sesión de Junta del próximo 30 de
      junio, se contará con información adicional valiosa.</p>
    </div>
  </div>
</body></html>
"""

class _FakeBanrepMinutasClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        if "minutas-banrep-marzo-2026" in url:
            return _FakeBinaryResponse(
                BANREP_MINUTAS_DETAIL_HTML.encode("utf-8"),
                url=url,
            )
        raise httpx.TransportError("detail unavailable")

class _FakeBanrepBotBlockClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        return _FakeBinaryResponse(
            b"<html><title>Radware Bot Manager</title></html>",
            url="https://validate.perfdrive.com/challenge",
        )

class _FakeBanrepBrowserPage:
    def __init__(self, html_by_url: dict[str, str]) -> None:
        self.html_by_url = html_by_url
        self.url = ""

    def goto(self, url, wait_until=None, timeout=None):  # noqa: ANN001
        self.url = url

    def wait_for_load_state(self, state, timeout=None):  # noqa: ANN001
        return None

    def content(self) -> str:
        return self.html_by_url[self.url]

class _FakeBinaryResponse:
    status_code = 200

    def __init__(self, content: bytes, headers=None, url: str = "https://example.com"):
        self.content = content
        self.headers = headers or {}
        self.url = url

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="ignore")

    def raise_for_status(self) -> None:
        return None

class _FakeBinaryClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        return _FakeBinaryResponse(_minimal_icoced_xlsx())

def _minimal_text_pdf(text: str) -> bytes:
    return (
        b"%PDF-1.4\n1 0 obj\n<<>>\nstream\nBT\n("
        + text.encode("latin-1")
        + b") Tj\nET\nendstream\nendobj\n%%EOF"
    )

def _minimal_operator_pdf(operator_stream: bytes) -> bytes:
    return (
        b"%PDF-1.4\n1 0 obj\n<<>>\nstream\n"
        + operator_stream
        + b"\nendstream\nendobj\n%%EOF"
    )

class _FakePdfClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        text = (
            "El DANE publica un comunicado tecnico con resultados economicos "
            "nacionales y suficiente texto para superar el umbral minimo."
        )
        return _FakeBinaryResponse(_minimal_text_pdf(text))

class _FakeSenadoPdfClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        text = (
            "MARTES 12 de mayo TEMA: la presentacion en primer debate del "
            "Proyecto de Ley No. 312 del 2025 Senado 463 del 2025 Camara, "
            "POR MEDIO DE LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO. "
            "Autores: Ministro de Hacienda. La agenda contiene informacion "
            "suficiente para el analista."
        )
        return _FakeBinaryResponse(_minimal_text_pdf(text))

MINCIT_ZF_SAMPLE_TEXT = (
    "ZONAS FRANCAS FECHA: 31 DE DICIEMBRE DE 2025 "
    "Fuente: Ministerio de Industria, Comercio y Turismo - DPC 29/01/2026 "
    "NIT NOMBRE ZONA FRANCA CLASE DE ZONA FRANCA TIPO DE USUARIO "
    "DEPARTAMENTO MUNICIPIO Resolución de declaratoria Resolución de prorroga CIIU "
    "800178052 Zona Franca Industrial de Bienes y Servicios La Candelaria "
    "Permanente Usuario Operador Bolívar Cartagena "
    "Res. 95 de 10 de febrero de 1993 Res. 1311 de 1 de diciembre de 2021 7020 "
    "90191119 3 Zona Franca Permanente Especial De Servicios Rionegro MRO "
    "Permanente especial Servicios Antioquia Rionegro "
    "Res. No. 2118 del 26 de diciembre de 2025 Vacía 3315"
)

def _mincit_zf_item() -> RawItem:
    return RawItem(
        id="mincit-zf-approved",
        source_id="mincit_zonas_francas",
        source_name="MinCIT — Zonas Francas (Estadísticas)",
        source_type="regulatory",
        url="https://zf.mincit.gov.co/getattachment/estadisticas/zonas.pdf.aspx",
        title="Zonas Francas aprobadas",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-02-18T00:00:00Z",
        raw_text="Zonas Francas aprobadas Fecha de actualización: 18 de febrero de 2026",
        metadata={"extraction": "anchor"},
    )

class _FakeMinCITPdfClient:
    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        return _FakeBinaryResponse(
            _minimal_operator_pdf(
                b"BT\n("
                + MINCIT_ZF_SAMPLE_TEXT.encode("latin-1")
                + b") Tj\nET"
            )
        )

class _FakeGacetaPdfClient:
    def __init__(self):
        self.posts = []

    def post(self, url, data=None):  # noqa: ANN001 - mirrors httpx.Client.post
        self.posts.append((url, data))
        text = (
            "Gaceta del Congreso 476. AL PROYECTO DE LEY NÚMERO 550 DE "
            "2026 CÁMARA Y SENADO por la cual se adiciona el Presupuesto "
            "General de la Nación de la vigencia fiscal de 2026. Página 1"
        )
        return _FakeBinaryResponse(
            _minimal_text_pdf(text),
            headers={"content-type": "application/pdf"},
        )

class _FakeDiarioPdfClient:
    def __init__(self):
        self.posts = []
        self.gets = []

    def post(self, url, data=None):  # noqa: ANN001 - mirrors httpx.Client.post
        self.posts.append((url, data))
        html = """
        <html><body>
          <object type="application/pdf"
            data="/diario/javax.faces.resource/dynamiccontent.properties.xhtml?ln=primefaces&amp;pfdrid=abc">
          </object>
        </body></html>
        """
        return _FakeBinaryResponse(
            html.encode("utf-8"),
            headers={"content-type": "text/html;charset=UTF-8"},
            url="https://svrpubindc.imprenta.gov.co/diario/view/detallesPdf.xhtml",
        )

    def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
        self.gets.append((url, params))
        text = (
            "Diario Oficial 53.490. Ministerio de Comercio, Industria y "
            "Turismo. Resolución 2118 de 2025 por la cual se declara la "
            "Zona Franca Permanente Especial De Servicios Rionegro MRO. "
            "Decreto 123 de 2026. Página 1"
        )
        return _FakeBinaryResponse(
            _minimal_text_pdf(text),
            headers={"content-type": "application/pdf"},
            url=url,
        )

class _FakeFeed:
    def __init__(self, entries):
        self.entries = entries
        self.feed = type("F", (), {"id": "feed-1"})()

__all__ = [name for name in globals() if not name.startswith("__")]
