import os
import pdfplumber
import json


def extract_text_from_pdf(pdf_path):
    text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text.strip()
    except Exception as e:
        print(f"错误：无法解析PDF文件 '{pdf_path}': {e}")
        return None

def process_pdfs_in_directory(pdf_directory, cache_directory):
    """
    处理指定目录中的所有PDF文件，提取文本，并使用缓存机制。

    Args:
        pdf_directory (str): 包含PDF文件的目录路径。
        cache_directory (str): 存储/读取提取文本JSON缓存的目录路径。

    Returns:
        dict: 一个字典，键是PDF文件名（不含扩展名），值是每个PDF提取的文本。
    """
    extracted_data = {}
    if not os.path.isdir(pdf_directory):
        print(f"错误：PDF目录 '{pdf_directory}' 不存在。")
        return extracted_data

    # 确保缓存目录存在
    if not os.path.exists(cache_directory):
        try:
            os.makedirs(cache_directory)
            print(f"已创建缓存目录: {cache_directory}")
        except OSError as e:
            print(f"错误：无法创建缓存目录 '{cache_directory}': {e}")
            # 如果无法创建缓存目录，则不使用缓存，但继续尝试解析
            pass


    for filename in os.listdir(pdf_directory):
        if filename.lower().endswith(".pdf"):
            paper_name = os.path.splitext(filename)[0] # 文件名作为论文名
            pdf_path = os.path.join(pdf_directory, filename)
            cache_file_path = os.path.join(cache_directory, f"{paper_name}.json")

            text_content = None

            # 1. 尝试从缓存加载
            if os.path.exists(cache_file_path):
                try:
                    with open(cache_file_path, 'r', encoding='utf-8') as f_cache:
                        cache_data = json.load(f_cache)
                        text_content = cache_data.get("text")
                        if text_content is not None:
                            print(f"已从缓存加载 '{paper_name}' 的文本。")
                        else:
                            print(f"警告：缓存文件 '{cache_file_path}' 格式不正确或缺少'text'字段。将重新解析。")
                except (IOError, json.JSONDecodeError) as e:
                    print(f"警告：读取或解析缓存文件 '{cache_file_path}' 失败: {e}。将重新解析PDF。")

            # 2. 如果缓存中没有或加载失败，则解析PDF
            if text_content is None:
                print(f"正在处理文件 (解析PDF): {pdf_path}...")
                text_content = extract_text_from_pdf(pdf_path)

                if text_content:
                    # 3. 如果解析成功，保存到缓存
                    if os.path.isdir(cache_directory):
                        try:
                            with open(cache_file_path, 'w', encoding='utf-8') as f_cache:
                                json.dump({"paper_name": paper_name, "text": text_content}, f_cache, ensure_ascii=False, indent=4)
                            print(f"已将 '{paper_name}' 的提取文本缓存到 '{cache_file_path}'。")
                        except IOError as e:
                            print(f"错误：无法写入缓存文件 '{cache_file_path}': {e}")
                    else:
                        print(f"警告：缓存目录 '{cache_directory}' 不可用，无法缓存 '{paper_name}' 的文本。")
                else:
                    print(f"未能从 {filename} 提取文本。")

            if text_content:
                extracted_data[paper_name] = text_content
            else:
                extracted_data[paper_name] = ""


    return extracted_data

if __name__ == '__main__':
    # 这是一个示例，展示如何使用此模块
    current_script_directory = os.path.dirname(os.path.abspath(__file__))
    pdf_folder = os.path.join(current_script_directory, "课程作业论文")
    # 定义缓存JSON文件的存储目录
    json_cache_folder = os.path.join(current_script_directory, "extracted_json_texts")

    # 确保 "课程作业论文" 文件夹存在，用于测试
    if not os.path.exists(pdf_folder):
        os.makedirs(pdf_folder)
        print(f"创建了示例PDF文件夹: {pdf_folder}")
        print("请放入PDF文件进行测试。")

    papers_text_content = process_pdfs_in_directory(pdf_folder, json_cache_folder)

    if papers_text_content:
        print(f"\n成功获取了 {len(papers_text_content)} 篇论文的文本内容。")
        for paper_name, text_snippet in papers_text_content.items():
            if text_snippet:
                print(f"\n--- {paper_name} (文本片段) ---")
                print(text_snippet[:200] + "...")
            else:
                print(f"\n--- {paper_name} (无文本内容) ---")
    else:
        print("在指定目录中没有找到PDF文件或无法处理它们。")
