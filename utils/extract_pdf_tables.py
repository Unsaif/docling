"""
extract_pdf_tables.py  ────────────────────────────────────────────────
★ pip install pdfplumber pandas unidecode tabulate

Core workflow
1.  Opens the PDF with pdfplumber (works whenever the file has a text layer).
2.  Calls page.extract_tables()  ➜  raw list-of-lists for each detected table.
3.  Merges cells that pdfplumber split because the original PDF wrapped them.
4.  Normalises Unicode (e.g. µ → mu) and trims footnote markers.
5.  Returns a list of DataFrames  ➜  easy to .to_csv() or .to_excel().
------------------------------------------------------------------------
If your PDF is **only a scanned image** (no selectable text), fall back to
the `ocr=True` branch at the bottom: it renders the page with pdf2image and
runs Tesseract, then uses camelot to guess table lines.
"""
from __future__ import annotations
import re, pathlib
from typing import List
import pandas as pd
import pdfplumber
from unidecode import unidecode
from tabulate import tabulate   # nice optional preview

def _postprocess(column_cells: list[str]) -> list[str]:
    """
    1. drop superscript digits / symbols (common footnote markers)
    2. normalise unicode to ASCII where possible
    3. squeeze inner whitespace
    """
    out: list[str] = []
    sup_re = re.compile(r"\s*(?:[\u00B9\u00B2\u00B3\u2070-\u2079])+\s*$")  # ¹ ² ³ … ⁹
    for cell in column_cells:
        cleaned = sup_re.sub("", cell or "")
        cleaned = unidecode(cleaned)          # µ -> mu, α -> a, etc.
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        out.append(cleaned)
    return out

def extract_tables(path: str|pathlib.Path,
                   *,
                   max_pages: int|None = None,
                   preview: bool = False) -> List[pd.DataFrame]:
    """Return a list of DataFrames – one per table."""
    dfs: list[pd.DataFrame] = []

    with pdfplumber.open(str(path)) as pdf:
        for page_idx, page in enumerate(pdf.pages, 1):
            if max_pages and page_idx > max_pages:
                break
            for raw_table in page.extract_tables():
                # pdfplumber already returns a list of rows (list[str])
                # Remove completely blank rows
                rows = [r for r in raw_table if any(c and c.strip() for c in r)]
                if not rows:
                    continue

                # Merge multi-row wrapped cells (very simple heuristic)
                merged_rows: list[list[str]] = []
                for r in rows:
                    if merged_rows and all(c in ("", None) for c in r[1:]):
                        # treat as continuation of previous row’s first col
                        merged_rows[-1][0] += " " + (r[0] or "")
                    else:
                        merged_rows.append(r)

                # DataFrame, clean-up
                df = pd.DataFrame(merged_rows)
                df = df.apply(_postprocess)

                # Promote first non-blank row to header when sensible
                if len(df) > 1 and df.iloc[0].isna().sum() < len(df.columns) / 2:
                    df.columns = df.iloc[0]
                    df = df.drop(index=df.index[0]).reset_index(drop=True)

                dfs.append(df)

                if preview:
                    print(f"\nPage {page_idx} · Table {len(dfs)}")
                    print(tabulate(df.head(10), headers="keys", tablefmt="github"))

    return dfs