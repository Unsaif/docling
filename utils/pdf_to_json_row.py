# pdf_to_json_row.py  –  Docling text  + full tables  →  JSON row
# ------------------------------------------------------------------
from __future__ import annotations
import json, pathlib
from typing import List
import pandas as pd
from dotenv import load_dotenv
from utils.extract_pdf_tables import extract_tables  # custom utility
from openai import OpenAI
from docling.document_converter import DocumentConverter     # the code you patched
import textwrap

load_dotenv()                                      # OPENAI_API_KEY=…

# ─── 1 · Field names + guidance ──────────────────────────────────────────
DESCRIPTORS = {
    "Gene"               : "responsible gene",
    "Summary"            : "5-8 lines summarising the disease",
    "Clinical_untreated" : "clinical phenotypes if untreated",
    "Clinical_treated"   : "clinical phenotypes if treated",
    "Variants"           : "which disease variants exist and the differences "
                           "in clinical and biochemical phenotypes",
    "Genetics"           : "loss- or gain-of-function? how many mutations "
                           "identified & validated",
    "Incidence"          : "overall incidence rate and, if possible, per country",
    "Diagnosis"          : "how is it diagnosed in newborn screening?",
    "Differential_diagnosis":
                           "what differential diagnoses exist?",
    "Treatment"          : "available treatments (be specific)",
    "Prognosis"          : "prognosis when identified and treated early",
}
COLUMNS = list(DESCRIPTORS.keys())

# ─── 2 · Convert PDF → (markdown_text, appended_table_block) ─────────────
def pdf_to_combined_markdown(pdf: str|pathlib.Path) -> str:
    # 2a.  main text from Docling
    conv      = DocumentConverter()
    md_main   = conv.convert(str(pdf)).document.export_to_markdown()

    # 2b.  extract every table, render each to markdown
    tables: List[pd.DataFrame] = extract_tables(pdf)
    md_tables = []
    for i, t in enumerate(tables, 1):
        md_tables.append(f"\n\n**Full Table {i}**\n\n" + t.to_markdown(index=False))

    # 2c.  concatenate with a notice for the LLM
    notice = textwrap.dedent("""
        ---
        **NOTE to the language-model:**  
        Some tables in the text above may be incomplete; the full versions
        extracted directly from the PDF are reproduced in a more complete manner below.
        ---
    """).strip()

    return md_main + "\n\n" + notice + "\n".join(md_tables)

# ─── 3 · Ask GPT to fill the JSON schema ────────────────────────────────
def combined_md_to_record(md_text: str,
                          *, model="gpt-4o-mini") -> dict:
    descriptor_block = "\n".join(
        f"**{k}** – {v}" for k, v in DESCRIPTORS.items()
    )
    schema = json.dumps({k: "…" for k in COLUMNS}, indent=2)

    client = OpenAI()
    r = client.chat.completions.create(
        model=model,
        messages=[{
            "role": "user",
            "content": [
                { "type": "text",
                  "text": (
                      "Using the disease report below, including the *full tables "
                      "appended at the end*, fill EVERY field of the JSON schema. "
                      "⚠️  Return ONLY valid JSON – no markdown.\n\n"
                      f"{schema}\n\n"
                      f"Field guidance:\n{descriptor_block}"
                  )},
                { "type": "text", "text": md_text }
            ]
        }],
        response_format={"type": "json_object"},
        # max_tokens=900,
    )
    return json.loads(r.choices[0].message.content)

# ─── 4 · End-to-end convenience function ────────────────────────────────
def pdf_to_dataframe(pdf_path: str|pathlib.Path,
                     *, model="gpt-4.1") -> pd.DataFrame:
    md   = pdf_to_combined_markdown(pdf_path)
    row  = combined_md_to_record(md, model=model)
    return pd.DataFrame([row], columns=COLUMNS)
