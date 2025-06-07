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
ORIGINAL_JSON_FILE = "valid_urls.json"
NEW_JSON_FILE = "deepseek.json"
MERGED_JSON_FILE = "merged_datasets.json"

def match_dataset_name(dataset_name: str, new_datasets: dict) -> str:
    """根据数据集名称的第一部分匹配新文件中的数据集名称。"""
    # 提取数据集名称的第一部分（以 _ 分割）
    name_prefix = dataset_name.split('_')[0]
    for new_name in new_datasets:
        if new_name.startswith(name_prefix):
            return new_name
    return None

def merge_datasets(original_data: dict, new_data: dict) -> dict:
    """将新 JSON 文件中的数据集合并到原始 JSON 文件中。"""
    merged_data = original_data.copy()
    
    for paper, datasets in new_data.items():
        if paper not in merged_data:
            merged_data[paper] = {}
        
        for dataset_name, info in datasets.items():
            # 检查原始数据中是否已有该数据集（通过名称前缀匹配）
            matched = False
            for orig_dataset in merged_data.get(paper, {}):
                if match_dataset_name(orig_dataset, {dataset_name: info}):
                    matched = True
                    break
            # 如果原始数据中不存在该数据集，添加
            if not matched:
                merged_data[paper][dataset_name] = info
    
    return merged_data

def main():
    cwd = os.path.abspath(os.path.dirname(__file__))
    original_path = os.path.join(cwd, ORIGINAL_JSON_FILE)
    new_path = os.path.join(cwd, NEW_JSON_FILE)
    merged_output_path = os.path.join(cwd, MERGED_JSON_FILE)

    # 检查输入文件是否存在
    if not os.path.exists(original_path):
        logging.error(f"原始文件 {original_path} 不存在")
        return
    if not os.path.exists(new_path):
        logging.error(f"新文件 {new_path} 不存在")
        return

    # 读取原始 JSON 文件
    try:
        with open(original_path, 'r', encoding='utf-8') as f:
            original_data = json.load(f)
    except Exception as e:
        logging.error(f"读取原始文件 {original_path} 失败：{e}")
        return

    # 读取新 JSON 文件
    try:
        with open(new_path, 'r', encoding='utf-8') as f:
            new_data = json.load(f)
    except Exception as e:
        logging.error(f"读取新文件 {new_path} 失败：{e}")
        return

    # 合并数据集
    merged_data = merge_datasets(original_data, new_data)

    # 保存合并后的数据
    try:
        with open(merged_output_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=4)
        logging.info(f"合并数据集已保存至 {merged_output_path}")
    except Exception as e:
        logging.error(f"保存合并数据集失败：{e}")

if __name__ == "__main__":
    main()