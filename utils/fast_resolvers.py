# utils/fast_resolvers.py
from __future__ import annotations
import re, hashlib
from typing import Dict, Optional
import requests

try:
    import diskcache as dc
    CACHE = dc.Cache("cache_resolvers")
except Exception:
    CACHE = {}

def _cache_get(k: str):
    try: return CACHE.get(k)
    except Exception: return None

def _cache_set(k: str, v, expire: int = 60*60*24*30):
    try:
        if hasattr(CACHE, "set"): CACHE.set(k, v, expire=expire)
        else: CACHE[k] = v
    except Exception: pass

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _session(user_agent: str = "case-extractor/1.0") -> requests.Session:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    retries = Retry(total=1, connect=1, read=1, backoff_factor=0.2,
                    status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"],
                    raise_on_status=False, respect_retry_after_header=True)
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=50))
    s.headers.update({"User-Agent": user_agent})
    return s

# ---- PubMed (exact title) ----
def resolve_pubmed_id_from_title(title: str) -> str:
    title = _norm(title)
    if not title: return ""
    key = "pmid:" + hashlib.sha1(title.encode("utf-8")).hexdigest()
    hit = _cache_get(key)
    if hit is not None: return hit
    try:
        s = _session()
        r = s.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                  params={"db":"pubmed","retmode":"json","retmax":"1","term":f"{title}[Title]"},
                  timeout=(2.0,5.0))
        r.raise_for_status()
        js = r.json() or {}
        ids = (js.get("esearchresult",{}) or {}).get("idlist",[]) or []
        pmid = ids[0] if ids else ""
    except Exception:
        pmid = ""
    _cache_set(key, pmid)
    return pmid

# ---- OMIM/Orphanet via Wikidata API claims + OLS fallback (no SPARQL) ----
def _mediawiki_exact_qid(s: requests.Session, label_en: str) -> Optional[str]:
    r = s.get("https://www.wikidata.org/w/api.php",
              params={"action":"wbsearchentities","format":"json","language":"en",
                      "search":label_en,"limit":5,"strictlanguage":1},
              timeout=(2.0,5.0))
    r.raise_for_status()
    hits = (r.json() or {}).get("search",[]) or []
    tgt = label_en.casefold()
    for h in hits:
        if (h.get("label") or "").casefold() == tgt:
            return h.get("id")
    return hits[0]["id"] if hits else None

def _claims_for_qid(s: requests.Session, qid: str) -> Dict[str,str]:
    r = s.get("https://www.wikidata.org/w/api.php",
              params={"action":"wbgetentities","format":"json","ids":qid,"props":"claims"},
              timeout=(2.0,5.0))
    r.raise_for_status()
    ent = (r.json() or {}).get("entities",{}).get(qid,{}).get("claims",{}) or {}
    def first(p): 
        try: return ent[p][0]["mainsnak"]["datavalue"]["value"]
        except Exception: return ""
    omim = str(first("P492") or "")
    orpha = str(first("P1550") or "")
    return {"OMIM": f"OMIM:{omim}" if omim else "", "OrphaNet": f"Orphanet:{orpha}" if orpha else ""}

def _ols_orphanet_exact(s: requests.Session, label: str) -> str:
    r = s.get("https://www.ebi.ac.uk/ols4/api/search",
              params={"q":label,"ontology":"ordo","queryFields":"label","exact":"true"},
              timeout=(2.0,5.0))
    if not r.ok: return ""
    js = r.json() or {}
    if js.get("response",{}).get("numFound",0) <= 0: return ""
    doc = js["response"]["docs"][0]
    curie = next((x for x in doc.get("obo_id",[]) if isinstance(x,str) and x.startswith("Orphanet_")), "")
    return curie.split("_",1)[-1] if curie else ""

def resolve_omim_and_orphanet_from_disease(label: str) -> Dict[str,str]:
    q = _norm(label)
    key = "ids:" + hashlib.sha1(q.encode("utf-8")).hexdigest()
    hit = _cache_get(key)
    if hit is not None: return hit
    if not q:
        out = {"OMIM":"","OrphaNet":""}
        _cache_set(key,out); return out
    s = _session()
    omim, orpha = "", ""
    try:
        qid = _mediawiki_exact_qid(s, q)
        if qid:
            ids = _claims_for_qid(s, qid)
            omim = ids.get("OMIM","") or ""
            orpha = ids.get("OrphaNet","") or ""
    except Exception:
        pass
    if not orpha:
        try:
            o = _ols_orphanet_exact(s, q)
            if o: orpha = f"Orphanet:{o}"
        except Exception:
            pass
    out = {"OMIM": omim, "OrphaNet": orpha}
    _cache_set(key, out)
    return out