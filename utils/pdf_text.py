# utils/pdf_text.py
from __future__ import annotations
from io import BytesIO
import pathlib, tempfile, contextlib
from typing import Union, Optional

from docling.document_converter import DocumentConverter

PDFLike = Union[str, pathlib.Path, bytes, bytearray, memoryview, BytesIO]

@contextlib.contextmanager
def _as_pdf_path(pdf: PDFLike, suffix: str = ".pdf"):
    if isinstance(pdf, (str, pathlib.Path)):
        yield str(pdf)
        return
    if isinstance(pdf, BytesIO):
        data = pdf.getvalue()
    elif isinstance(pdf, (bytes, bytearray, memoryview)):
        data = bytes(pdf)
    else:
        raise TypeError(f"Unsupported PDF input type: {type(pdf)!r}")
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
        tmp.write(data)
        tmp.flush()
        yield tmp.name

def _try_docling_markdown_from_bytes(data: bytes) -> Optional[str]:
    conv = DocumentConverter()
    convert_bytes = getattr(conv, "convert_bytes", None)
    if convert_bytes is None:
        return None
    doc = convert_bytes(data)  # type: ignore
    return doc.document.export_to_markdown()

def _try_docling_markdown_from_path(p: str) -> str:
    conv = DocumentConverter()
    doc = conv.convert(p)
    return doc.document.export_to_markdown()

def pdf_to_markdown_text(pdf: PDFLike, *, use_pymupdf_fallback: bool = True) -> str:
    """
    Convert PDF to markdown using Docling; fallback to PyMuPDF plain text.
    Raises RuntimeError if everything yields empty text.
    """
    # 1) bytes route (if supported)
    try:
        if isinstance(pdf, BytesIO):
            md = _try_docling_markdown_from_bytes(pdf.getvalue())
            if isinstance(md, str) and md.strip():
                return md
        elif isinstance(pdf, (bytes, bytearray, memoryview)):
            md = _try_docling_markdown_from_bytes(bytes(pdf))
            if isinstance(md, str) and md.strip():
                return md
    except Exception as e:
        # don't print; let caller handle
        _last_err = e  # for possible reporting below

    # 2) path route
    try:
        with _as_pdf_path(pdf) as p:
            md = _try_docling_markdown_from_path(p)
            if isinstance(md, str) and md.strip():
                return md
    except Exception as e:
        _last_err = e

    # 3) fallback: PyMuPDF plain text
    if use_pymupdf_fallback:
        try:
            import fitz  # PyMuPDF
            if isinstance(pdf, BytesIO):
                doc = fitz.open(stream=pdf.getvalue(), filetype="pdf")
            elif isinstance(pdf, (bytes, bytearray, memoryview)):
                doc = fitz.open(stream=bytes(pdf), filetype="pdf")
            else:
                with _as_pdf_path(pdf) as p:
                    doc = fitz.open(p)
            parts = [page.get_text() for page in doc]
            doc.close()
            text = "\n\n".join(parts).strip()
            if text:
                return text
        except Exception as e:
            _last_err = e

    # If we got here, everything produced empty
    raise RuntimeError(f"PDF→text produced empty output"
                       f"{'; last error: ' + repr(_last_err) if '_last_err' in locals() else ''}")