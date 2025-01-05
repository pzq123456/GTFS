import os
import requests
from dotenv import load_dotenv
import tqdm
import pandas as pd
import zipfile
# 加载环境变量
load_dotenv()

# 从 .env 文件获取 Refresh Token
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
TOKEN_URL = "https://api.mobilitydatabase.org/v1/tokens"
API_BASE_URL = "https://api.mobilitydatabase.org/v1"


# 数据路径设置
DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data')

# 1. Germany
DATA_PATH = os.path.join(DIR, 'GTFS', 'Germany')
SAVE_PATH = os.path.join(DATA_PATH, 'output')

# 2. Italy
DATA_PATH2 = os.path.join(DIR, 'GTFS', 'Italy')
SAVE_PATH2 = os.path.join(DATA_PATH2, 'output')

# 3. Netherlands
DATA_PATH3 = os.path.join(DIR, 'GTFS', 'Netherlands')
SAVE_PATH3 = os.path.join(DATA_PATH3, 'output')

# 4. Norway
DATA_PATH4 = os.path.join(DIR, 'GTFS', 'Norway')
SAVE_PATH4 = os.path.join(DATA_PATH4, 'output')

# 5. Poland
DATA_PATH5 = os.path.join(DIR, 'GTFS', 'Poland')
SAVE_PATH5 = os.path.join(DATA_PATH5, 'output')

# 6. Portugal
DATA_PATH6 = os.path.join(DIR, 'GTFS', 'Portugal')
SAVE_PATH6 = os.path.join(DATA_PATH6, 'output')

# 7. Spain
DATA_PATH7 = os.path.join(DIR, 'GTFS', 'Spain')
SAVE_PATH7 = os.path.join(DATA_PATH7, 'output')

# 8. Sweden
DATA_PATH8 = os.path.join(DIR, 'GTFS', 'Sweden')
SAVE_PATH8 = os.path.join(DATA_PATH8, 'output')

# 9. Switzerland
DATA_PATH9 = os.path.join(DIR, 'GTFS', 'Switzerland')
SAVE_PATH9 = os.path.join(DATA_PATH9, 'output')

DIRS = [DATA_PATH, DATA_PATH2, DATA_PATH3, DATA_PATH4, DATA_PATH5, DATA_PATH6, DATA_PATH7, DATA_PATH8, DATA_PATH9] # 9 countries

# 编写代码检查对应文件夹是否存在，如果不存在则创建文件夹
def check_dir(dirs):
    for dir in tqdm.tqdm(dirs):
        if not os.path.exists(dir):
            os.makedirs(dir)

# Germany 21 Metros
# Italy 8 Metros
# Netherlands 3 Metros
# Norway 1 Metros Poland 2 Metros
# Portugal 2 Metros
# Spain 9 Metros Sweden 1 Metros
# Switzerland 1 Metros

def get_access_token(refresh_token):
    """
    使用 Refresh Token 获取 Access Token
    """
    try:
        response = requests.post(
            TOKEN_URL,
            headers={"Content-Type": "application/json"},
            json={"refresh_token": refresh_token}
        )
        response.raise_for_status()  # 检查请求是否成功
        data = response.json()
        return data.get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"获取 Access Token 时发生错误: {e}")
        return None

def access_api(endpoint, access_token, params=None):
    """
    使用 Access Token 访问 API 资源
    """
    try:
        url = f"{API_BASE_URL}/{endpoint}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # 检查请求是否成功
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"访问 API 资源时发生错误: {e}")
        return None

def fetch_data_from_api(endpoint, params=None):
    """
    封装的函数：自动刷新 Access Token 并访问 API
    """
    # 获取 Access Token
    access_token = get_access_token(REFRESH_TOKEN)
    if not access_token:
        print("无法获取 Access Token，终止操作。")
        return None

    # 使用 Access Token 访问 API
    data = access_api(endpoint, access_token, params)
    if data is None:
        print("无法访问 API，终止操作。")
    return data

def download_file(url, save_path):
    """
    下载文件并保存到指定路径
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(save_path, "wb") as file:
            file.write(response.content)
        print(f"文件已下载并保存到: {save_path}")
    except requests.exceptions.RequestException as e:
        print(f"下载文件时发生错误: {e}")

import os

def download_gtfs_data_for_country(country_code, save_dir="gtfs_data"):
    """
    下载指定国家的所有 GTFS 数据，并存储在以数字命名的子文件夹中。
    """
    # 创建保存目录
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # 获取指定国家的 GTFS feeds
    endpoint = "gtfs_feeds"
    params = {"country_code": country_code}
    feeds = fetch_data_from_api(endpoint, params)

    if not feeds:
        print(f"未找到国家代码为 {country_code} 的 GTFS feeds。")
        return

    print(f"找到 {len(feeds)} 个 GTFS feeds。")

    # 遍历每个 feed 并下载最新数据集
    for index, feed in enumerate(feeds):
        feed_id = feed.get("id")
        print(f"处理 feed ID: {feed_id}")

        # 获取该 feed 的最新数据集
        endpoint = f"gtfs_feeds/{feed_id}/datasets"
        params = {"latest": True}
        datasets = fetch_data_from_api(endpoint, params)

        if not datasets or len(datasets) == 0:
            print(f"未找到 feed ID {feed_id} 的数据集。")
            continue

        # 下载最新数据集
        latest_dataset = datasets[0]
        dataset_url = latest_dataset.get("hosted_url")
        if not dataset_url:
            print(f"未找到 feed ID {feed_id} 的数据集下载链接。")
            continue

        # 创建以数字命名的子文件夹
        subfolder_path = os.path.join(save_dir, str(index))
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)

        # 保存文件
        file_name = f"{feed_id}.zip"
        save_path = os.path.join(subfolder_path, file_name)
        print(f"正在下载数据集到 {subfolder_path}: {dataset_url}")
        download_file(dataset_url, save_path)

def unzip_file(file_path):
    """
    解压文件到当前目录
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(file_path))

def get_subdirs(dir_path):
    """
    获取指定目录下的所有子文件夹路径（非递归）
    """
    return [os.path.join(dir_path, name) for name in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, name))]

def merge_txt_files_by_name(input_dir, output_dir):
    """
    合并每个子文件夹中同名的 TXT 文件到指定输出目录
    """
    # 创建输出目录（如果不存在）
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 获取子文件夹列表
    subdirs = get_subdirs(input_dir)
    print(f"找到 {len(subdirs)} 个子文件夹需要处理。")

    # 收集所有文件的名称
    all_files = set()
    for subdir in subdirs:
        for file_name in os.listdir(subdir):
            if file_name.endswith(".txt"):
                all_files.add(file_name)

    # 遍历所有文件名，逐个合并
    for file_name in tqdm.tqdm(all_files, desc="合并文件"):
        merged_data = []

        # 遍历子文件夹中的同名文件
        for subdir in subdirs:
            file_path = os.path.join(subdir, file_name)
            if os.path.exists(file_path):
                # 读取 TXT 文件为 DataFrame
                df = pd.read_csv(file_path, low_memory=False, dtype=str)
                merged_data.append(df)

        # 合并所有 DataFrame
        if merged_data:
            merged_df = pd.concat(merged_data, ignore_index=True)
            # 保存到输出目录
            output_path = os.path.join(output_dir, file_name)
            merged_df.to_csv(output_path, index=False)
            # print(f"文件 {file_name} 已合并并保存到 {output_path}")

def process_gtfs_data(input_dir, output_dir):
    """
    解压每个子文件夹中的压缩包并合并 TXT 文件
    """
    subdirs = get_subdirs(input_dir)
    print(f"找到 {len(subdirs)} 个子文件夹进行解压。")

    # 解压每个子文件夹中的压缩包
    for subdir in tqdm.tqdm(subdirs, desc="解压文件"):
        for file_name in os.listdir(subdir):
            if file_name.endswith(".zip"):
                file_path = os.path.join(subdir, file_name)
                unzip_file(file_path)

    # 合并解压后的 TXT 文件
    merge_txt_files_by_name(input_dir, output_dir)

# 示例调用
if __name__ == "__main__":
    # 1. 创建文件夹
    # check_dir(DIRS)

    # 2. 下载 GTFS 数据并处理
    # country_code = "IT"  # 意大利的国家代码 Italy
    # download_gtfs_data_for_country(country_code, save_dir=DATA_PATH2)
    process_gtfs_data(DATA_PATH2, DATA_PATH2)
