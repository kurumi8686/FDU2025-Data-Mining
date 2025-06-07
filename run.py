import os, re, json, math, time, logging
from typing import List, Callable, Any

from pdf_parser import process_pdfs_in_directory
from llm_agent import extract_datasets_from_text
from dataset_resolver import DatasetResolver

# ============== 全局参数（可按需调整） ==============
PDF_DIRECTORY_NAME   = "课程作业论文1"
CACHED_TEXTS_DIR     = "extract"
OUTPUT_JSON_FILE     = "dataset_extraction_results.json"

API_CHOICE           = "paid"      # 透传给 llm_agent.extract_datasets_from_text
MODEL_MAX_TOKENS     = 3000       # 单块最多 token（≤ 模型上限）
LLM_RETRIES          = 3           # LLM / 网络调用重试次数
NETWORK_RETRIES      = 3
INITIAL_DELAY        = 2           # 首次失败后延迟秒数
BACKOFF_FACTOR       = 2           # 指数退避倍率
RESOLVE_TIMEOUT      = 10          # dataset_resolver 联网超时

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)

resolver = DatasetResolver()
_WORDS_PER_TOKEN = 0.75           # 粗略估计：英文 0.75 词 ≈ 1 token

# ----------------------------------------------------
#                通用工具
# ----------------------------------------------------
def token_estimate(text: str) -> int:
    """不用 tiktoken，快速估算 token 数。"""
    return math.ceil(len(text.split()) / _WORDS_PER_TOKEN)

def call_with_retry(func: Callable[..., Any], /, *args,
                    retries: int = LLM_RETRIES,
                    initial_delay: int = INITIAL_DELAY,
                    backoff: int = BACKOFF_FACTOR,
                    **kwargs):
    """带指数退避的重试装饰器。"""
    delay = initial_delay
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == retries:
                raise
            logging.warning("调用 %s 第 %d/%d 次失败：%s；%d 秒后重试",
                            func.__name__, attempt, retries, e, delay)
            time.sleep(delay)
            delay *= backoff

# ----------------------------------------------------
#           切块策略：先按页，再按段
# ----------------------------------------------------
_heading_pat = re.compile(
    r"""(?imx)         # ignorecase | multiline | verbose
    ^\s*               # 行首空白
    (?:                # (I) “1.”、“1.1.”、“I.”、“II.”...
        (\d{1,2}(\.\d{1,2})*|[IVX]{1,4})[.)]\s+
      |                # (II) 典型章节词，如 "ABSTRACT", "CONCLUSION"
        (ABSTRACT|INTRODUCTION|RELATED\s+WORKS?|METHODS?|EXPERIMENTS?|RESULTS?|
         DISCUSSION|CONCLUSION|ACKNOWLEDGMENTS?|REFERENCES)\s*
    )
    [A-Z].{0,80}$      # 后面必须还有正文（全大写/首字大写）
""")

def _split_long_chunk(chunk: str, max_tokens: int, min_tokens: int = None) -> List[str]:
    """
    拆分超长文本块，优先按标题，其次按段落，尽量让每块接近 max_tokens。
    """
    if min_tokens is None:
        min_tokens = int(0.75 * max_tokens)

    # 1) 按标题分割（如章节、小节）
    idxs = [0] + [m.start() for m in _heading_pat.finditer(chunk)] + [len(chunk)]
    subsecs = [chunk[idxs[i]:idxs[i + 1]].strip() for i in range(len(idxs) - 1)]
    subsecs = [s for s in subsecs if s]

    if len(subsecs) > 1 and max(token_estimate(s) for s in subsecs) < max_tokens:
        # 合并 subsecs 使得每块 ≈ max_tokens
        pieces, buf, buf_tokens = [], [], 0
        for s in subsecs:
            tks = token_estimate(s)
            if buf_tokens + tks > max_tokens and buf_tokens >= min_tokens:
                pieces.append("\n\n".join(buf))
                buf, buf_tokens = [s], tks
            else:
                buf.append(s)
                buf_tokens += tks
        if buf:
            pieces.append("\n\n".join(buf))
        return pieces

    # 2) 按段落拆（双换行）
    paras = [p.strip() for p in chunk.split("\n\n") if p.strip()]
    pieces, buf, buf_tokens = [], [], 0
    for p in paras:
        tks = token_estimate(p)
        if buf_tokens + tks > max_tokens and buf_tokens >= min_tokens:
            pieces.append("\n\n".join(buf))
            buf, buf_tokens = [p], tks
        else:
            buf.append(p)
            buf_tokens += tks
    if buf:
        pieces.append("\n\n".join(buf))
    return pieces

def split_into_chunks(text: str, max_tokens: int = MODEL_MAX_TOKENS) -> List[str]:
    pages = re.split(r"\f", text)
    if len(pages) <= 1:
        logging.warning("未检测到换页符，强制按字符数分割")
        pages = [text[i:i+5000] for i in range(0, len(text), 5000)]

    logging.debug("初始页面数: %d", len(pages))
    chunks, buf, buf_tokens = [], [], 0
    for pg in pages:
        tks = token_estimate(pg)
        if tks > max_tokens:
            logging.debug("单页 %d tokens > max_tokens，细分", tks)
            chunks.extend(_split_long_chunk(pg, max_tokens))
            continue
        if buf_tokens + tks > max_tokens and buf:
            chunks.append("\f".join(buf))
            buf, buf_tokens = [pg], tks
        else:
            buf.append(pg)
            buf_tokens += tks

    if buf:
        chunks.append("\f".join(buf))

    final_chunks = []
    for ck in chunks:
        while token_estimate(ck) > max_tokens:
            logging.debug("块 %d tokens > max_tokens，强制细分", token_estimate(ck))
            ck_splits = _split_long_chunk(ck, max_tokens)
            if len(ck_splits) == 1 and token_estimate(ck_splits[0]) > max_tokens:
                logging.warning("标题/段落分割无效，强制按字符数分割")
                ck_splits = [ck[i:i+2000] for i in range(0, len(ck), 2000)]
            ck = ck_splits[0]
            final_chunks.extend(ck_splits[1:])
        final_chunks.append(ck)

    logging.debug("最终块数: %d, 各块token数: %s", len(final_chunks), [token_estimate(ck) for ck in final_chunks])
    return final_chunks

# ----------------------------------------------------
#                URL 补全（带重试）
# ----------------------------------------------------
def enrich_with_urls(dataset_dict: dict[str, list]) -> dict[str, list]:
    for name, info in dataset_dict.items():
        if len(info) < 3:
            info.extend(["N/A"] * (3 - len(info)))
        if info[1] in ("", "N/A", "null", None, "Not specified", "URL redacted"):
            url = resolver.resolve(name, no_fetch=True)
            if not url:    # 只有联网时才做重试
                url = call_with_retry(
                    resolver.resolve, name,
                    retries=NETWORK_RETRIES,
                    initial_delay=INITIAL_DELAY,
                    backoff=BACKOFF_FACTOR,
                    timeout=RESOLVE_TIMEOUT
                )
            if url:
                info[1] = url
    return dataset_dict

def aggregate_datasets(chunk_results: List[dict[str, list]]) -> dict[str, list]:
    merged: dict[str, list] = {}
    for res in chunk_results:
        for k, v in res.items():
            merged.setdefault(k, v)
    return merged

# ----------------------------------------------------
#                        主程序
# ----------------------------------------------------
def main():
    cwd = os.path.abspath(os.path.dirname(__file__))
    pdf_folder   = os.path.join(cwd, PDF_DIRECTORY_NAME)
    cache_folder = os.path.join(cwd, CACHED_TEXTS_DIR)
    output_path  = os.path.join(cwd, OUTPUT_JSON_FILE)

    # 1) 提取 / 缓存 pdf 文本
    papers_text = process_pdfs_in_directory(pdf_folder, cache_folder)
    if not papers_text:
        logging.error("未获取到任何论文文本，退出。")
        return

    all_results: dict[str, dict[str, list]] = {}

    # 2) 逐篇论文处理
    for paper, full_txt in papers_text.items():
        logging.info("⇨ 处理《%s》", paper)
        chunks = split_into_chunks(full_txt, MODEL_MAX_TOKENS)
        tot_tokens = sum(token_estimate(c) for c in chunks)
        logging.info("  ▶ 拆成 %d 块（估计 %d tokens）", len(chunks), tot_tokens)

        chunk_results = []
        for idx, ck in enumerate(chunks, 1):
            logging.debug("    • LLM chunk %d/%d", idx, len(chunks))
            try:
                res = call_with_retry(
                    extract_datasets_from_text,
                    f"{paper} – chunk {idx}", ck,
                    api_choice=API_CHOICE,
                    retries=LLM_RETRIES,
                    initial_delay=INITIAL_DELAY,
                    backoff=BACKOFF_FACTOR,
                )
                chunk_results.append(res or {})
            except Exception as e:
                logging.warning("      ✗ LLM 失败 chunk %d：%s", idx, e)

        merged   = aggregate_datasets(chunk_results)
        enriched = enrich_with_urls(merged)
        ok_count = sum(1 for v in enriched.values() if v[1] != "N/A")
        logging.info("  ▶ 识别 %d 个数据集，成功解析 URL %d 个", len(enriched), ok_count)
        all_results[paper] = enriched

    # 3) 保存
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=4)
        logging.info("✔ 结果已写入 %s", output_path)
    except Exception as e:
        logging.error("保存结果失败：%s", e)

if __name__ == "__main__":
    main()