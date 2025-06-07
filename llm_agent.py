# llm_agent.py
import json
import requests
from openai import OpenAI

# --- 付费API配置 ---
PAID_API_KEY = "key"
PAID_API_ENDPOINT_URL = "https://api.vveai.com/v1/chat/completions"
DEFAULT_PAID_MODEL = "gpt-4o"

# --- 免费API配置 ---
DEEPSEEK_API_KEY = "key"
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"

def call_paid_llm_api(prompt_text, model_name=DEFAULT_PAID_MODEL, temperature=0.2):
    actual_model_name = model_name
    current_temperature = temperature
    if "#" in model_name:
        temp_parts = model_name.split("#")
        actual_model_name = temp_parts[0]
        try:
            current_temperature = float(temp_parts[1])
        except ValueError:
            print(f"警告：模型名称中的温度格式无效 '{model_name}'。使用默认温度 {temperature}。")
    params = {
        "messages": [{"role": "user", "content": prompt_text}],
        "model": actual_model_name,
        "temperature": current_temperature,
    }
    headers = {
        "Authorization": "Bearer " + PAID_API_KEY,
        "Content-Type": "application/json",
    }

    print(f"付费API调用：模型={actual_model_name}, 温度={current_temperature}")
    try:
        response = requests.post(
            PAID_API_ENDPOINT_URL,
            headers=headers,
            json=params,
            stream=False,
            timeout=240
        )
        response.raise_for_status()
        res_json = response.json()
        if "choices" in res_json and res_json["choices"] and "message" in res_json["choices"][0] and "content" in \
                res_json["choices"][0]["message"]:
            message = res_json["choices"][0]["message"]["content"]
            return message
        else:
            print(f"错误：付费API响应格式意外。响应: {res_json}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"错误：付费API请求失败: {e}")
        print(f"响应内容: {response.text}")
        return None
    except json.JSONDecodeError:
        print(f"错误：无法解码付费API的JSON响应。响应文本: {response.text}")
        return None


def call_free_llm_api(prompt_text, model_name=DEFAULT_DEEPSEEK_MODEL, temperature=0.0):
    try:
        client = OpenAI(
            base_url=DEEPSEEK_BASE_URL,
            api_key=DEEPSEEK_API_KEY
        )
        print(f"DeepSeek API调用：模型={model_name}, 温度={temperature}")
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system",
                 "content": "You are a helpful assistant specialized in extracting dataset information from research papers."},
                {"role": "user", "content": prompt_text}
            ],
            temperature=temperature
        )
        if response.choices and response.choices[0].message and response.choices[0].message.content:
            return response.choices[0].message.content
        else:
            print(f"错误：DeepSeek API响应格式意外。响应: {response}")
            return None
    except Exception as e:
        print(f"错误：DeepSeek API调用失败: {e}")
        return None


def construct_dataset_extraction_prompt(paper_text_content):
    prompt = f"""
请仔细分析以下研究论文的文本内容。您的任务是识别文本中提及的所有数据集（包括没有出现url的数据集）。
对于每个识别出的数据集，请提取以下信息：
1.  "dataset_name": 数据集的标准名称。
2.  "platform": 数据集托管的平台（例如："GitHub"、"Hugging Face"、"Official Website"、"Kaggle"、"Paper's Repository"等）。
3.  "url": 指向数据集的完整官方链接。如果链接指向的是包含多个数据集的通用存储库（如某个GitHub组织），请尽可能找到最具体的数据集链接。
4.  "description": 对数据集的简要描述（可选，如果文本中提供）。

请将提取的信息格式化为一个JSON字符串。此JSON字符串应为一个字典，其中每个顶级键是数据集的名称 ("dataset_name")。
对应的值应该是另一个包含 "platform"、"url" 和 "description" 键的字典。
例如，如果找到一个名为 "AwesomeDataset" 的数据集，其部分JSON输出应如下所示：
{{
  "AwesomeDataset": {{
    "platform": "GitHub",
    "url": "https://github.com/user/awesomedataset",
    "description": "一个用于完成出色任务的数据集。"
  }}
}}

如果找到多个数据集，请将它们全部包含在主JSON对象中。
如果没有在文本中找到任何数据集，请返回一个空的JSON对象字符串，即 "{{}}".

以下是研究论文的文本内容：
---开始文本---
{paper_text_content}
---结束文本---

请确保您的回复严格遵循所请求的JSON格式。
"""
    return prompt


def extract_datasets_from_text(paper_name, text_content, api_choice="free", **kwargs):
    """
    使用LLM从给定的文本内容中提取数据集信息。

    Args:
        paper_name (str): 论文的名称（用于日志记录）。
        text_content (str): 从PDF中提取的文本。
        api_choice (str): "paid" 或 "free"，选择要使用的API。
                          当为 "free" 时，现在将调用配置为DeepSeek的API。
        **kwargs: 传递给特定API函数的附加参数 (例如 model_name, temperature)。

    Returns:
        dict: 一个字典，其中键是数据集名称，值是包含平台、URL和描述的列表。
              例如：{ "DatasetName": ["platform", "url", "description"] }
              如果未找到数据集或发生错误，则返回空字典。
    """
    prompt = construct_dataset_extraction_prompt(text_content)
    llm_response_str = None

    print(f"\n正在为论文 '{paper_name}' 查询LLM ({api_choice} API)...")

    if api_choice == "paid":
        model_name = kwargs.get("paid_model_name", DEFAULT_PAID_MODEL)
        temperature = kwargs.get("paid_temperature", 0.2)
        llm_response_str = call_paid_llm_api(prompt, model_name=model_name, temperature=temperature)
    elif api_choice == "free":  # 现在 "free" 选项会调用 call_free_llm_api，该函数已配置为使用DeepSeek
        model_name = kwargs.get("free_model_name", DEFAULT_DEEPSEEK_MODEL)  # 默认使用DeepSeek模型
        temperature = kwargs.get("free_temperature", 0.0)
        llm_response_str = call_free_llm_api(prompt, model_name=model_name, temperature=temperature)
    else:
        print(f"错误：无效的API选择 '{api_choice}'。请选择 'paid' 或 'free'。")
        return {}

    if not llm_response_str:
        print(f"未能从LLM获取论文 '{paper_name}' 的响应。")
        return {}

    print(f"LLM原始响应片段 ({paper_name}):\n{llm_response_str[:500]}...")

    try:
        if llm_response_str.strip().startswith("```json"):
            llm_response_str = llm_response_str.strip()[7:]
            if llm_response_str.strip().endswith("```"):
                llm_response_str = llm_response_str.strip()[:-3]

        parsed_llm_output = json.loads(llm_response_str.strip())

        formatted_datasets = {}
        if isinstance(parsed_llm_output, dict):
            for ds_name, ds_info in parsed_llm_output.items():
                if isinstance(ds_info, dict):
                    platform = ds_info.get("platform", "N/A")
                    url = ds_info.get("url", "N/A")
                    description = ds_info.get("description", "")  # 默认为空字符串
                    formatted_datasets[ds_name] = [platform, url, description]
                else:
                    print(f"警告：论文 '{paper_name}' 的数据集 '{ds_name}' 的LLM输出格式不正确：{ds_info}")

            if formatted_datasets:
                print(f"成功为论文 '{paper_name}' 解析了 {len(formatted_datasets)} 个数据集。")
            else:
                print(f"在论文 '{paper_name}' 的LLM响应中未找到有效的数据集条目，或响应为空。")
            return formatted_datasets
        else:
            print(f"错误：LLM为论文 '{paper_name}' 返回的不是预期的字典格式。响应: {parsed_llm_output}")
            return {}

    except json.JSONDecodeError as e:
        print(f"错误：无法解码来自LLM的JSON响应 ({paper_name})。错误: {e}")
        print(f"LLM响应原文: {llm_response_str}")
        return {}
    except Exception as e:
        print(f"处理LLM响应时发生意外错误 ({paper_name}): {e}")
        return {}


if __name__ == '__main__':
    sample_text_content = """
    在这项工作中，我们介绍了CodeSearchNet数据集，这是一个用于代码搜索的大规模数据集。
    我们还使用了HumanEval数据集 (https://github.com/openai/human-eval) 进行评估。
    另一个相关的数据集是MBPP，可以在 https://github.com/google-research/google-research/tree/master/mbpp 找到。
    我们还参考了ALFWorld (https://github.com/alfworld/alfworld.git)，它是一个基于ALFRED的交互式任务完成数据集。
    """
    print("--- 测试付费API ---")
    # 注意：这将进行实际的API调用，如果密钥和端点配置正确
    # extracted_info_paid = extract_datasets_from_text("sample_paper_paid", sample_text_content, api_choice="paid")
    # if extracted_info_paid:
    #     print("从付费API提取的数据集:")
    #     print(json.dumps(extracted_info_paid, indent=2, ensure_ascii=False))
    # else:
    #     print("未能使用付费API提取数据集。")

    print("\n--- 测试免费API ---")
    # 注意：这将进行实际的API调用，前提是DeepSeek的常量已正确配置
    # 请确保 DEEPSEEK_API_KEY 和 DEEPSEEK_BASE_URL 已正确设置
    # extracted_info_deepseek = extract_datasets_from_text("sample_paper_deepseek", sample_text_content, api_choice="free")
    # if extracted_info_deepseek:
    #     print("从DeepSeek API提取的数据集:")
    #     print(json.dumps(extracted_info_deepseek, indent=2, ensure_ascii=False))
    # else:
    #     print("未能使用DeepSeek API提取数据集。请检查API密钥和端点配置。")

    # 测试空响应
    empty_text_content = "这篇论文讨论了理论概念，但没有提及任何具体的数据集。"
    # extracted_info_empty = extract_datasets_from_text("empty_paper", empty_text_content, api_choice="free") # 同样会使用DeepSeek
    # print("\n测试空文本的提取 (使用DeepSeek):")
    # print(json.dumps(extracted_info_empty, indent=2, ensure_ascii=False))
    print("提醒: main函数的测试部分已被注释掉，以防止意外的API调用。")
    print("请取消注释并确保API密钥已配置，以进行实际测试。")
