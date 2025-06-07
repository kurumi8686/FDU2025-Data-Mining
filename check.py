import json
import os
import re
import logging

# 配置日志
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)

# 全局参数
INPUT_JSON_FILE = "merged_datasets.json"
VALID_URLS_FILE = "valid_urls1.json"
INVALID_URLS_FILE = "invalid_urls1.json"

# 正则表达式匹配以 http:// 或 https:// 开头的 URL
VALID_URL_PATTERN = re.compile(r'^https?://[\w\-\.]+.*$')

def is_valid_url(url: str) -> bool:
    """检查 URL 是否以 http:// 或 https:// 开头。"""
    if not url or url == "N/A":
        return False
    return bool(VALID_URL_PATTERN.match(url))

def complete_url(url: str) -> str:
    """为以 // 开头的 URL 补全协议（优先 https，若无效则 http）。"""
    if not url.startswith('//'):
        return url
    # 尝试 https
    https_url = f"https:{url}"
    if is_valid_url(https_url):
        return https_url
    # 回退到 http
    http_url = f"http:{url}"
    return http_url

def process_urls_in_json(input_path: str, valid_output_path: str, invalid_output_path: str):
    """处理 JSON 文件中的 URL，补全并分类保存。"""
    try:
        # 读取输入 JSON 文件
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"读取输入文件 {input_path} 失败：{e}")
        return

    valid_urls = {}
    invalid_urls = {}

    # 遍历 JSON 数据
    for paper, datasets in data.items():
        valid_urls[paper] = {}
        invalid_urls[paper] = {}
        for dataset, info in datasets.items():
            url = info[1]  # URL 在列表的第二个位置
            if url == "N/A" or not url:
                invalid_urls[paper][dataset] = info
                continue

            # 补全 URL
            completed_url = complete_url(url)
            # 验证 URL
            if is_valid_url(completed_url):
                valid_urls[paper][dataset] = info.copy()
                valid_urls[paper][dataset][1] = completed_url
            else:
                invalid_urls[paper][dataset] = info
                logging.warning(f"无效 URL: {url} (补全后: {completed_url})")

    # 直接覆盖保存有效 URL
    try:
        with open(valid_output_path, 'w', encoding='utf-8') as f:
            json.dump(valid_urls, f, ensure_ascii=False, indent=4)
        logging.info(f"有效 URL 已保存至 {valid_output_path}")
    except Exception as e:
        logging.error(f"保存有效 URL 失败：{e}")

    # 直接覆盖保存无效 URL
    try:
        with open(invalid_output_path, 'w', encoding='utf-8') as f:
            json.dump(invalid_urls, f, ensure_ascii=False, indent=4)
        logging.info(f"无效 URL 已保存至 {invalid_output_path}")
    except Exception as e:
        logging.error(f"保存无效 URL 失败：{e}")

def main():
    cwd = os.path.abspath(os.path.dirname(__file__))
    input_path = os.path.join(cwd, INPUT_JSON_FILE)
    valid_output_path = os.path.join(cwd, VALID_URLS_FILE)
    invalid_output_path = os.path.join(cwd, INVALID_URLS_FILE)

    if not os.path.exists(input_path):
        logging.error(f"输入文件 {input_path} 不存在")
        return

    process_urls_in_json(input_path, valid_output_path, invalid_output_path)

if __name__ == "__main__":
    main()