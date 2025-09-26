# # pdf_to_json_row.py  –  Docling text  + full tables  →  JSON row
# # ------------------------------------------------------------------
# from __future__ import annotations
# import json, pathlib
# from typing import List
# import pandas as pd
# from dotenv import load_dotenv
# from utils.extract_pdf_tables import extract_tables  # custom utility
# from openai import OpenAI
# from docling.document_converter import DocumentConverter     # the code you patched
# import textwrap

# load_dotenv()                                      # OPENAI_API_KEY=…

# # ─── 1 · Field names + guidance ──────────────────────────────────────────
# DESCRIPTORS = {
#     "Gene"               : "responsible gene",
#     "Summary"            : "5-8 lines summarising the disease",
#     "Clinical_untreated" : "clinical phenotypes if untreated",
#     "Clinical_treated"   : "clinical phenotypes if treated",
#     "Variants"           : "which disease variants exist and the differences "
#                            "in clinical and biochemical phenotypes",
#     "Genetics"           : "loss- or gain-of-function? how many mutations "
#                            "identified & validated",
#     "Incidence"          : "overall incidence rate and, if possible, per country",
#     "Diagnosis"          : "how is it diagnosed in newborn screening?",
#     "Differential_diagnosis":
#                            "what differential diagnoses exist?",
#     "Treatment"          : "available treatments (be specific)",
#     "Prognosis"          : "prognosis when identified and treated early",
# }
# COLUMNS = list(DESCRIPTORS.keys())

# # ─── 2 · Convert PDF → (markdown_text, appended_table_block) ─────────────
# def pdf_to_combined_markdown(pdf: str|pathlib.Path) -> str:
#     # 2a.  main text from Docling
#     conv      = DocumentConverter()
#     md_main   = conv.convert(str(pdf)).document.export_to_markdown()

#     # 2b.  extract every table, render each to markdown
#     tables: List[pd.DataFrame] = extract_tables(pdf)
#     md_tables = []
#     for i, t in enumerate(tables, 1):
#         md_tables.append(f"\n\n**Full Table {i}**\n\n" + t.to_markdown(index=False))

#     # 2c.  concatenate with a notice for the LLM
#     notice = textwrap.dedent("""
#         ---
#         **NOTE to the language-model:**  
#         Some tables in the text above may be incomplete; the full versions
#         extracted directly from the PDF are reproduced in a more complete manner below.
#         ---
#     """).strip()

#     return md_main + "\n\n" + notice + "\n".join(md_tables)

# # ─── 3 · Ask GPT to fill the JSON schema ────────────────────────────────
# def combined_md_to_record(md_text: str,
#                           *, model="gpt-4o-mini") -> dict:
#     descriptor_block = "\n".join(
#         f"**{k}** – {v}" for k, v in DESCRIPTORS.items()
#     )
#     schema = json.dumps({k: "…" for k in COLUMNS}, indent=2)

#     client = OpenAI()
#     r = client.chat.completions.create(
#         model=model,
#         messages=[{
#             "role": "user",
#             "content": [
#                 { "type": "text",
#                   "text": (
#                       "Using the disease report below, including the *full tables "
#                       "appended at the end*, fill EVERY field of the JSON schema. "
#                       "⚠️  Return ONLY valid JSON – no markdown.\n\n"
#                       f"{schema}\n\n"
#                       f"Field guidance:\n{descriptor_block}"
#                   )},
#                 { "type": "text", "text": md_text }
#             ]
#         }],
#         response_format={"type": "json_object"},
#         # max_tokens=900,
#     )
#     return json.loads(r.choices[0].message.content)

# # ─── 4 · End-to-end convenience function ────────────────────────────────
# def pdf_to_dataframe(pdf_path: str|pathlib.Path,
#                      *, model="gpt-4.1") -> pd.DataFrame:
#     md   = pdf_to_combined_markdown(pdf_path)
#     row  = combined_md_to_record(md, model=model)
#     return pd.DataFrame([row], columns=COLUMNS)

# pdf_to_json_row_cases.py  –  Docling text + full tables → JSON row (case-centric)
# -------------------------------------------------------------------------------------------------
from __future__ import annotations
import json, pathlib, textwrap, re
from typing import List, Optional, Dict
import pandas as pd
import requests
from dotenv import load_dotenv
from openai import OpenAI
from docling.document_converter import DocumentConverter           # your patched Docling
from utils.extract_pdf_tables import extract_tables                # your custom utility
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Optional
from typing import Union
from io import BytesIO
import pathlib
import tempfile
import contextlib
import textwrap

load_dotenv()  # expects OPENAI_API_KEY

# ────────────────────────────────────────────────────────────────────────────────────────────────
# 1) Target schema for the new task
DESCRIPTORS = {
    "Case_description"   : "Extract the patient/case description. Omit any references to figures/tables. "
                           "Do NOT include any genetic information. Do NOT include the disease name.",
    "Genetic_validation" : "Does the PDF report genetic validation? Answer 'yes' or 'no'.",
    "Responsible_gene"   : "Responsible gene as reported in the paper (symbol, as written).",
    "Underlying_disease" : "Underlying disease name as reported in the paper.",
    "OMIM"               : "OMIM ID for the underlying disease (e.g., 'OMIM:123456'). Retrieved from the internet.",
    "OrphaNet"           : "Orphanet ID for the underlying disease (e.g., 'Orphanet:123'). Retrieved from the internet.",
    "Reference_title"    : "Paper title.",
    "PubMed_ID"          : "PMID resolved from the title via PubMed.",
    "Single-patient case report"    : "Does the report refer to a single patient? Answer 'yes' or 'no'.",
}
COLUMNS = list(DESCRIPTORS.keys())

# Types your function will accept
PDFInput = Union[str, pathlib.Path, bytes, BytesIO]

@contextlib.contextmanager
def _as_pdf_path(pdf: PDFInput, suffix: str = ".pdf"):
    """
    Context manager that yields a filesystem path to the PDF.
    - If `pdf` is already a path-like, yields it directly.
    - If `pdf` is bytes or BytesIO, writes to a NamedTemporaryFile and yields that path.
    File is cleaned up on exit.
    """
    if isinstance(pdf, (str, pathlib.Path)):
        yield str(pdf)
        return

    # bytes or BytesIO -> temp file
    if isinstance(pdf, BytesIO):
        data = pdf.getvalue()
    elif isinstance(pdf, (bytes, bytearray, memoryview)):
        data = bytes(pdf)
    else:
        raise TypeError(f"Unsupported PDF input type: {type(pdf)!r}")

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
        tmp.write(data)
        tmp.flush()
        yield tmp.name  # path-like expected by libs
        # tmp is auto-deleted on context exit

# ────────────────────────────────────────────────────────────────────────────────────────────────
# 2) PDF → (markdown_text with appended full-table block)  [unchanged idea]
def pdf_to_combined_markdown(pdf: PDFInput) -> str:
    """
    Convert a PDF (path or in-memory bytes/BytesIO) to Markdown, and append
    full table extracts below the main text.

    Parameters
    ----------
    pdf : str | pathlib.Path | bytes | io.BytesIO
        Path to a PDF, or the PDF file bytes/stream.

    Returns
    -------
    str
        Combined Markdown with a notice + tables rendered as Markdown.
    """
    # --- Main text -----------------------------------------------------------

    try:
        # Prefer a bytes-based conversion if available
        if isinstance(pdf, (bytes, bytearray, memoryview)):
            conv = DocumentConverter()
            md_main = conv.convert_bytes(bytes(pdf)).document.export_to_markdown()  # if your lib supports it
        elif isinstance(pdf, BytesIO):
            conv = DocumentConverter()
            md_main = conv.convert_bytes(pdf.getvalue()).document.export_to_markdown()
        else:
            # Fall back to path mode
            with _as_pdf_path(pdf) as pdf_path:
                conv = DocumentConverter()
                md_main = conv.convert(str(pdf_path)).document.export_to_markdown()
    except AttributeError:
        # Library doesn't support bytes -> always go through a temp path
        with _as_pdf_path(pdf) as pdf_path:
            conv = DocumentConverter()
            md_main = conv.convert(str(pdf_path)).document.export_to_markdown()

    # --- Tables --------------------------------------------------------------
    # Give extract_tables the same flexibility by always handing it a path via the helper.
    with _as_pdf_path(pdf) as pdf_path_for_tables:
        tables: List[pd.DataFrame] = extract_tables(pdf_path_for_tables)

    md_tables: List[str] = []
    for i, t in enumerate(tables, 1):
        md_tables.append(f"\n\n**Full Table {i}**\n\n" + t.to_markdown(index=False))

    notice = textwrap.dedent("""
        ---
        **NOTE to the language-model:**  
        Some tables in the text above may be incomplete; the full versions
        extracted directly from the PDF are reproduced in a more complete manner below.
        ---
    """).strip()

        # --- Detect if this is a case report -----------------------------
    is_case_report = False
    if md_main:
        lowered = md_main.lower()
        if "case report" in lowered:
            is_case_report = True

    # --- Tables ------------------------------------------------------
    with _as_pdf_path(pdf) as pdf_path_for_tables:
        tables: List[pd.DataFrame] = extract_tables(pdf_path_for_tables)

    md_tables: List[str] = []
    for i, t in enumerate(tables, 1):
        md_tables.append(f"\n\n**Full Table {i}**\n\n" + t.to_markdown(index=False))

    notice = textwrap.dedent("""
        ---
        **NOTE to the language-model:**  
        Some tables in the text above may be incomplete; the full versions
        extracted directly from the PDF are reproduced in a more complete manner below.
        ---
    """).strip()

    result = md_main + "\n\n" + notice + ("\n".join(md_tables) if md_tables else "")

    if is_case_report:
        result = "**[This document is a CASE REPORT]**\n\n" + result
    else: 
        raise ValueError("The document does not appear to be a case report.")

    return result

    # return md_main + "\n\n" + notice + ("\n".join(md_tables) if md_tables else "")

# ────────────────────────────────────────────────────────────────────────────────────────────────
# 3) LLM prompt → draft record (JSON)
PROMPT_INSTRUCTIONS = textwrap.dedent("""
    Could you please assist me with the following task? I would like to fill the following table
    given the attached PDF.

    Please follow these rules carefully:

    • Case description (as in paper): Extract the case/patient description while omitting all references
      to figures/tables. Do NOT include any genetic information. Do NOT include any naming of the disease.

    • Genetic validation: Does the PDF report genetic validation (yes/no)? Please use the exact wording as in the paper

    • Responsible gene (as in paper): Which gene does the paper report to be responsible?

    • Underlying disease (as in paper): Provide the disease name exactly as reported in the paper.

    • OMIM: Leave blank for now. This will be retrieved from the internet using the underlying disease name.

    • OrphaNet: Leave blank for now. This will be retrieved from the internet using the underlying disease name.

    • Reference: Use the title of the paper; we will obtain the PubMed ID from the internet using this title.
                                      
    • PubMed ID: Leave blank for now.

    • Single-patient case report: Does the report refer to a single patient (yes/no)?

    Return ONLY valid JSON with the following keys and nothing else:
""").strip()

def combined_md_to_record(md_text: str, *, model="gpt-4o-mini") -> Dict[str, str]:
    # skeleton with ellipses so we force all keys to appear
    skeleton = {k: "…" for k in COLUMNS}
    schema   = json.dumps(skeleton, indent=2)

    descriptor_block = "\n".join(f"**{k}** – {v}" for k, v in DESCRIPTORS.items())

    client = OpenAI()
    r = client.chat.completions.create(
        model=model,
        messages=[{
            "role": "user",
            "content": [
                { "type": "text",
                  "text": (
                    f"{PROMPT_INSTRUCTIONS}\n\n{schema}\n\nField guidance:\n{descriptor_block}"
                  )},
                { "type": "text", "text": md_text }
            ]
        }],
        response_format={"type": "json_object"},
    )
    data = json.loads(r.choices[0].message.content)

    # Normalize keys and strip ellipses if any remain
    out = {k: (data.get(k, "") or "").strip() for k in COLUMNS}
    for k,v in out.items():
        if v == "…":
            out[k] = ""
    return out

# ────────────────────────────────────────────────────────────────────────────────────────────────
# 4) Lightweight web resolvers (no keys required)
#    - PubMed PMID from title (NCBI E-utilities)
#    - OMIM & Orphanet from disease name (Wikidata SPARQL; fallback OLS for Orphanet)

def _http_get_json(url: str, params: dict, timeout: float = 12.0) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def resolve_pubmed_id_from_title(title: str) -> str:
    if not title:
        return ""
    # Exact-title search bias
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "pubmed",
        "retmode": "json",
        "retmax": "1",
        "term": f"{title}[Title]"
    }
    js = _http_get_json(base, params)
    try:
        ids = js["esearchresult"]["idlist"]
        return ids[0] if ids else ""
    except Exception:
        return ""

def _clean_label(label: str) -> str:
    return re.sub(r"\s+", " ", (label or "")).strip()

def _requests_session(user_agent: str = "pdf-to-json-row/1.0 (you@example.com)") -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=1, connect=3, read=5, backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": user_agent})
    return s

def _mediawiki_exact_qid(s: requests.Session, label_en: str) -> Optional[str]:
    r = s.get(
        "https://www.wikidata.org/w/api.php",
        params={
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "search": label_en,
            "limit": 5,
            "strictlanguage": 1
        },
        timeout=(5, 30)
    )
    r.raise_for_status()
    hits = r.json().get("search", [])
    target = _normalize_for_match(label_en)

    # 1) exact label match ignoring case
    for h in hits:
        if _normalize_for_match(h.get("label", "")) == target:
            return h.get("id")

    # 2) otherwise, take the top hit (optional but practical)
    return hits[0]["id"] if hits else None

def _claims_for_qid(s: requests.Session, qid: str) -> Dict[str, str]:
    r = s.get(
        "https://www.wikidata.org/w/api.php",
        params={
            "action": "wbgetentities",
            "format": "json",
            "ids": qid,
            "props": "claims"
        },
        timeout=(5, 30)
    )
    r.raise_for_status()
    ent = r.json()["entities"][qid].get("claims", {})
    def first(prop):
        try:
            return ent[prop][0]["mainsnak"]["datavalue"]["value"]
        except Exception:
            return ""
    omim = str(first("P492") or "")
    orpha = str(first("P1550") or "")
    return {"OMIM": f"OMIM:{omim}" if omim else "", "OrphaNet": f"Orphanet:{orpha}" if orpha else ""}

def _normalize_for_match(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return s.casefold()  # better than lower() for Unicode

def resolve_omim_and_orphanet_from_disease(label: str) -> Dict[str, str]:
    """
    Returns {"OMIM": "OMIM:123456", "OrphaNet": "Orphanet:123"} (or blanks if not found).
    Strategy: MediaWiki exact label -> SPARQL fallback -> EBI OLS Orphanet fallback.
    """
    q = _clean_label(label)
    if not q:
        return {"OMIM": "", "OrphaNet": ""}

    s = _requests_session()

    # 1) MediaWiki exact label → QID → claims
    try:
        qid = _mediawiki_exact_qid(s, q)
        if qid:
            ids = _claims_for_qid(s, qid)
            if ids["OMIM"] or ids["OrphaNet"]:
                return ids
    except Exception as e:
        print(f"⚠️ MediaWiki lookup failed for '{q}': {e}")

    # 2) SPARQL fallback (exact English label)
    omim_id, orpha_id = "", ""
    q_escaped = q.replace('"', '\\"')
    sparql = f"""
    SELECT ?omim ?orpha WHERE {{
    ?d rdfs:label ?label .
    FILTER(LANG(?label) = "en")
    FILTER(LCASE(STR(?label)) = LCASE("{q_escaped}"))
    OPTIONAL {{ ?d wdt:P492 ?omim. }}
    OPTIONAL {{ ?d wdt:P1550 ?orpha. }}
    }}
    LIMIT 1
    """
    try:
        resp = s.post(
            "https://query.wikidata.org/sparql",
            data={"query": sparql},
            headers={"Accept": "application/sparql-results+json"},
            timeout=(5, 60)
        )
        resp.raise_for_status()
        rows = resp.json()["results"]["bindings"]
        if rows:
            r0 = rows[0]
            omim_id = (r0.get("omim", {}) or {}).get("value", "") or ""
            orpha_id = (r0.get("orpha", {}) or {}).get("value", "") or ""
    except Exception as e:
        print(f"⚠️ SPARQL failed for '{q}': {e}")

    # 3) Orphanet fallback via EBI OLS if missing
    if not orpha_id:
        try:
            ols = s.get(
                "https://www.ebi.ac.uk/ols4/api/search",
                params={"q": q, "ontology": "ordo", "queryFields": "label", "exact": "true"},
                timeout=(5, 30)
            )
            if ols.ok:
                js = ols.json()
                if js.get("response", {}).get("numFound", 0) > 0:
                    doc = js["response"]["docs"][0]
                    curie = next((x for x in doc.get("obo_id", []) if x.startswith("Orphanet_")), "")
                    if curie:
                        orpha_id = curie.split("_", 1)[-1]
        except Exception:
            pass

    # Normalize prefixes and return (single return point, consistent type)
    omim = f"OMIM:{omim_id}" if omim_id else ""
    orpha = f"Orphanet:{orpha_id}" if orpha_id else ""
    return {"OMIM": omim, "OrphaNet": orpha}

# ────────────────────────────────────────────────────────────────────────────────────────────────
# 5) End-to-end convenience
def pdf_to_dataframe_cases(pdf_path: str | pathlib.Path, *, model="gpt-4.1") -> pd.DataFrame:
    md   = pdf_to_combined_markdown(pdf_path)
    row  = combined_md_to_record(md, model=model)

    # Derive PMID from title (we store title separately for lookup, then put back)
    title = row.get("Reference_title", "") or row.get("Reference", "")
    pmid  = resolve_pubmed_id_from_title(title)
    if pmid:
        row["PubMed_ID"] = pmid

    # Resolve OMIM / Orphanet from the disease name
    disease = row.get("Underlying_disease", "")
    ids = resolve_omim_and_orphanet_from_disease(disease)
    row["OMIM"]     = ids.get("OMIM", "")     or row.get("OMIM", "")
    row["OrphaNet"] = ids.get("OrphaNet", "") or row.get("OrphaNet", "")

    # Final column order and single-row DataFrame
    return pd.DataFrame([ {k: row.get(k, "") for k in COLUMNS} ], columns=COLUMNS)

# ────────────────────────────────────────────────────────────────────────────────────────────────
# 6) Tiny CLI helper (optional)
# if __name__ == "__main__":
#     import sys
#     if len(sys.argv) < 2:
#         print("Usage: python pdf_to_json_row_cases.py <path/to/file.pdf>")
#         raise SystemExit(2)
#     df = pdf_to_dataframe_cases(sys.argv[1])
#     # Show as JSON on stdout
#     print(df.to_json(orient="records", force_ascii=False, indent=2))
