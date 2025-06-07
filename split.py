import os
import json
import re

# 定义输入和输出文件夹路径
input_folder = "extracted_json_texts"
before_refs_folder = "before_references"
after_refs_folder = "after_references"

# 确保输出文件夹存在
os.makedirs(before_refs_folder, exist_ok=True)
os.makedirs(after_refs_folder, exist_ok=True)

# 遍历输入文件夹中的所有 JSON 文件
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        input_filepath = os.path.join(input_folder, filename)
        
        # 读取 JSON 文件
        try:
            with open(input_filepath, 'r', encoding='utf-8', newline='') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue
        
        # 检查 text 字段
        text = data.get('text', '')
        if not text:
            print(f"Warning: Empty text field in {filename}")
            continue
        
        # 规范化换行符
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        # 初始化分割结果
        before_references = text
        after_references = ''
        
        # 首先尝试使用 REFERENCES 分割
        references_pattern = r'REFERENCES'
        split_result = re.split(references_pattern, text, maxsplit=1)
        
        if len(split_result) > 1:
            # 成功使用 REFERENCES 分割
            before_references = split_result[0]
            after_references = split_result[1]
            print(f"Split {filename} using 'REFERENCES'")
        else:
            # 未找到 REFERENCES，尝试使用 References 分割
            references_pattern = r'References'
            split_result = re.split(references_pattern, text, maxsplit=1)
            if len(split_result) > 1:
                before_references = split_result[0]
                after_references = split_result[1]
                print(f"Split {filename} using 'References'")
            else:
                print(f"No 'REFERENCES' or 'References' found in {filename}, saving entire text as before_references")
        
        # 如果 after_references 为空，记录警告
        if not after_references.strip():
            print(f"Warning: No content after References in {filename}")
        
        # 准备保存的 JSON 数据
        before_json = {
            "paper_name": data.get('paper_name', ''),
            "text": before_references.strip()
        }
        after_json = {
            "paper_name": data.get('paper_name', ''),
            "text": after_references.strip()
        }
        
        # 定义输出文件路径
        base_filename = os.path.splitext(filename)[0]
        before_output_filepath = os.path.join(before_refs_folder, f"{base_filename}.json")
        after_output_filepath = os.path.join(after_refs_folder, f"{base_filename}.json")
        
        # 保存文件
        try:
            with open(before_output_filepath, 'w', encoding='utf-8') as f:
                json.dump(before_json, f, ensure_ascii=False, indent=4)
            print(f"Saved before_references to {before_output_filepath}")
            
            # 仅当 after_references 非空时保存
            if after_references.strip():
                with open(after_output_filepath, 'w', encoding='utf-8') as f:
                    json.dump(after_json, f, ensure_ascii=False, indent=4)
                print(f"Saved after_references to {after_output_filepath}")
        except Exception as e:
            print(f"Error saving files for {filename}: {e}")

print("JSON 文件分割完成！")