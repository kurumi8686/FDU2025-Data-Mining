import re, os, json, time, sqlite3, html, requests, urllib.parse
from contextlib import closing
import logging
logger = logging.getLogger(__name__)

PWC_API = "https://paperswithcode.com/api/v0/datasets/{}"
HF_API  = "https://huggingface.co/api/datasets?search={}"
DDG_API = "https://duckduckgo.com/html/?q={}"

def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9\-]+", "", s.lower().replace(" ", "-"))

class DatasetResolver:
    def __init__(self, db: str = "dataset_cache.sqlite", verbose: bool = True):
        self.conn = sqlite3.connect(db)
        self._ensure()
        self.verbose = verbose

    def _ensure(self):
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS url_cache (name TEXT PRIMARY KEY, url TEXT, ts REAL)"
        )

    def _get(self, name):
        cur = self.conn.execute("SELECT url FROM url_cache WHERE name=?", (name,))
        row = cur.fetchone()
        return row[0] if row else None

    def _save(self, name, url):
        self.conn.execute(
            "INSERT OR REPLACE INTO url_cache(name, url, ts) VALUES(?,?,?)",
            (name, url, time.time()),
        )
        self.conn.commit()

    def resolve(self, name: str, *, no_fetch=False, **opt) -> str | None:
        name = name.strip()
        cached = self._get(name)
        if cached:
            if self.verbose:
                print(f"[cache] {name} -> {cached}")
            return cached
        if no_fetch:
            if self.verbose:
                print(f"[cache-miss] {name} (no_fetch=True)")
            return None
        url = (
            self._try("PapersWithCode", self._from_pwc, name, **opt) or
            self._try("HuggingFace",   self._from_hf,  name, **opt) or
            self._try("DuckDuckGo",    self._from_ddg, name, **opt) or
            self._try("Kaggle",        self._from_kaggle, name, **opt) or
            self._try("GoogleDS",      self._from_google_ds, name, **opt) or
            self._try("PWC-Search",    self._from_pwc_search, name, **opt) or
            self._try("GitHub",        self._from_github, name, **opt)
        )
        if url:
            self._save(name, url)
        return url

    def _try(self, label: str, fn, *a, **kw):
        try:
            url = fn(*a, **kw)
            if self.verbose:
                print(f"[{label}] {a[0]} -> {url or 'None'}")
            return url
        except Exception as e:
            logger.warning("[%s] 解析 %s 失败: %s", label, a[0], e)
            return None

    def _from_pwc(self, name, **opt):
        slug = _slugify(name)
        r = requests.get(PWC_API.format(slug), timeout=opt.get("timeout", 8))
        if r.status_code == 200:
            return r.json().get("url")
        return None

    def _from_hf(self, name, **opt):
        q = urllib.parse.quote(name)
        r = requests.get(HF_API.format(q), timeout=opt.get("timeout", 8),
                         headers={"Accept": "application/json"})
        try:
            arr = r.json()
            if arr:
                return f"https://huggingface.co/datasets/{arr[0]['id']}"
        except ValueError:
            pass
        return None

    def _from_ddg(self, name, **opt):
        q = urllib.parse.quote_plus(f"{name} dataset")
        html_txt = requests.get(DDG_API.format(q), timeout=opt.get("timeout", 8)).text
        m = re.search(r'nofollow" class="[^"]+" href="([^"]+)"', html_txt)
        return html.unescape(m.group(1)) if m else None

    def _from_kaggle(self, name, **opt):
        q = urllib.parse.quote_plus(f"{name} site:kaggle.com")
        html_txt = requests.get(DDG_API.format(q), timeout=opt.get("timeout", 8)).text
        m = re.search(r'nofollow" class="[^"]+" href="([^"]+)"', html_txt)
        return html.unescape(m.group(1)) if m else None

    def _from_google_ds(self, name, **opt):
        q = urllib.parse.quote_plus(f"{name} site:datasetsearch.research.google.com")
        html_txt = requests.get(DDG_API.format(q), timeout=opt.get("timeout", 8)).text
        m = re.search(r'nofollow" class="[^"]+" href="([^"]+)"', html_txt)
        return html.unescape(m.group(1)) if m else None

    def _from_pwc_search(self, name, **opt):
        q = urllib.parse.quote_plus(f"{name} site:paperswithcode.com/datasets")
        html_txt = requests.get(DDG_API.format(q), timeout=opt.get("timeout", 8)).text
        m = re.search(r'nofollow" class="[^"]+" href="([^"]+)"', html_txt)
        return html.unescape(m.group(1)) if m else None

    def _from_github(self, name, **opt):
        q = urllib.parse.quote_plus(f"{name} dataset site:github.com")
        html_txt = requests.get(DDG_API.format(q), timeout=opt.get("timeout", 8)).text
        m = re.search(r'nofollow" class="[^"]+" href="([^"]+)"', html_txt)
        return html.unescape(m.group(1)) if m else None
