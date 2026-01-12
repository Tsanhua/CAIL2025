# -*- coding = utf-8 -*-
# @Time : 2025/11/13 22:44
# @Author : 刘赞华
# @File : workflow.py
# @Software : PyCharm
import json
import requests
import time
import os
import argparse  # ==================== 新增导入 ====================
from typing import Dict, Any, List, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
import math

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== 禁用系统代理 ====================
os.environ['NO_PROXY'] = '*'
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

# ==================== 配置参数 ====================
ACCESS_TOKEN = "xxxxxxxxxx"
WORKFLOW_ID = "xxxxxxxxxx"
INPUT_FILE = "test_file_path"
OUTPUT_FILE = "prediction.jsonl"

# 多进程配置
NUM_PROCESSES = 4  # 🔧 可修改进程数

# API配置
API_URL = "https://api.coze.cn/v1/workflow/run"
MAX_RETRIES = 3
TIMEOUT = 240
RETRY_DELAY = 5
VERIFY_SSL = False


# ==================== 创建Session ====================
def create_session():
    """创建带重试机制的Session，禁用代理"""
    session = requests.Session()
    session.proxies = {'http': None, 'https': None, 'no_proxy': '*'}
    session.trust_env = False

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


# ==================== 处理输出格式 ====================
def process_output(data_str: str) -> Dict[str, Any]:
    """处理API返回的data字段，提取并格式化输出"""
    try:
        if isinstance(data_str, str):
            data_obj = json.loads(data_str)
        else:
            data_obj = data_str

        if isinstance(data_obj, dict) and 'output' in data_obj:
            output_str = data_obj['output']
            if isinstance(output_str, str):
                output_obj = json.loads(output_str)
            else:
                output_obj = output_str
        else:
            output_obj = data_obj

        result = OrderedDict()
        if 'id' in output_obj:
            result['id'] = output_obj['id']
        if 'answer1' in output_obj:
            result['answer1'] = output_obj['answer1']
        if 'answer2' in output_obj:
            result['answer2'] = output_obj['answer2']

        for key in sorted(output_obj.keys()):
            if key not in result:
                result[key] = output_obj[key]

        return dict(result)

    except (json.JSONDecodeError, TypeError) as e:
        return {"raw_output": data_str}


# ==================== 调用工作流 ====================
def call_workflow(session: requests.Session, id_value: Any, fact: str) -> Dict[str, Any]:
    """调用工作流API，带超时重试机制"""
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    payload = {
        "workflow_id": WORKFLOW_ID,
        "parameters": {
            "id": id_value,
            "fact": fact
        }
    }

    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(
                API_URL,
                headers=headers,
                json=payload,
                timeout=TIMEOUT,
                verify=VERIFY_SSL,
                proxies={'http': None, 'https': None}
            )

            response.raise_for_status()
            result = response.json()

            if result.get("code") == 0:
                data_str = result.get("data", "")
                return process_output(data_str)
            else:
                error_msg = result.get("msg", "Unknown error")
                error_code = result.get("code")

                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    raise Exception(f"API错误 (code: {error_code}): {error_msg}")

        except requests.exceptions.Timeout:
            last_exception = TimeoutError("请求超时")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

        except Exception as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    if last_exception:
        raise Exception(f"达到最大重试次数: {str(last_exception)}")
    else:
        raise Exception("未知错误，处理失败")


# ==================== 处理单个任务 ====================
def process_single_task(task_info: Tuple[int, int, Any, str]) -> Tuple[bool, int, int, Any, Dict, str]:
    """
    处理单个任务

    Args:
        task_info: (index, line_num, id_value, fact)

    Returns:
        (success, index, line_num, id_value, result_dict, error_msg)
    """
    index, line_num, id_value, fact = task_info
    session = create_session()

    try:
        result = call_workflow(session, id_value, fact)
        return (True, index, line_num, id_value, result, "")
    except Exception as e:
        error_msg = str(e)
        return (False, index, line_num, id_value, {}, error_msg)
    finally:
        session.close()
        time.sleep(0.5)  # 避免请求过快


# ==================== 批处理函数 ====================
def process_batch(batch_tasks: List[Tuple], progress_dict: Dict, lock, process_id: int) -> List[Tuple]:
    """
    处理一批任务

    Args:
        batch_tasks: 任务列表
        progress_dict: 共享进度字典
        lock: 进程锁
        process_id: 进程ID

    Returns:
        结果列表
    """
    results = []

    for idx, task in enumerate(batch_tasks, 1):
        result = process_single_task(task)
        results.append(result)

        # 更新进度
        with lock:
            progress_dict[process_id] = idx
            total = sum(progress_dict.values())
            print(f"\r进程{process_id} [{idx}/{len(batch_tasks)}] | 总进度: {total}",
                  end="", flush=True)

    return results


# ==================== 读取输入文件 ====================
def load_input_data(filepath: str) -> List[Tuple[int, int, Any, str]]:
    """
    读取输入文件

    Returns:
        [(index, line_num, id_value, fact), ...]
    """
    tasks = []

    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
                id_value = data.get("id")
                fact = data.get("fact")

                if id_value is not None and fact is not None:
                    tasks.append((len(tasks), line_num, id_value, fact))

            except json.JSONDecodeError:
                continue

    return tasks


# ==================== 分割任务 ====================
def split_tasks(tasks: List, num_chunks: int) -> List[List]:
    """将任务分割成多个批次"""
    chunk_size = math.ceil(len(tasks) / num_chunks)
    return [tasks[i:i + chunk_size] for i in range(0, len(tasks), chunk_size)]


# ==================== 主函数 ====================
def main():
    """主函数"""
    # ==================== 新增：解析命令行参数 ====================
    parser = argparse.ArgumentParser(description='Workflow Process')
    parser.add_argument('--pred_file', type=str, default=INPUT_FILE, help='Input file path')
    args = parser.parse_args()
    current_input_file = args.pred_file
    # ==========================================================

    print("=" * 70)
    print("工作流API调用程序 - 多进程版本")
    print("=" * 70)
    print(f"输入文件: {current_input_file}")
    print(f"输出文件: {OUTPUT_FILE}")
    print(f"进程数量: {NUM_PROCESSES}")
    print(f"工作流ID: {WORKFLOW_ID}")
    print(f"超时设置: {TIMEOUT}秒, 最大重试: {MAX_RETRIES}次")
    print("=" * 70)
    print()

    # 读取输入数据
    print("📖 正在读取输入文件...")
    try:
        tasks = load_input_data(current_input_file)
        print(f"  ✓ 共加载 {len(tasks)} 个任务")
    except FileNotFoundError:
        print(f"✗ 错误: 找不到输入文件 '{current_input_file}'")
        return
    except Exception as e:
        print(f"✗ 文件读取失败: {str(e)}")
        return

    if not tasks:
        print("✗ 没有有效的任务数据")
        return

    # 分割任务
    batches = split_tasks(tasks, NUM_PROCESSES)
    print(f"\n📦 任务分配:")
    for i, batch in enumerate(batches, 1):
        print(f"  进程{i}: {len(batch)} 个任务")
    print()

    # 确保输出目录存在
    output_dir = os.path.dirname(OUTPUT_FILE)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 多进程处理
    print("🚀 开始多进程处理...")
    print("=" * 70)

    start_time = time.time()
    all_results = []

    # 使用Manager创建共享对象
    manager = Manager()
    progress_dict = manager.dict({i: 0 for i in range(1, len(batches) + 1)})
    lock = manager.Lock()

    try:
        with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
            futures = {
                executor.submit(process_batch, batch, progress_dict, lock, idx): idx
                for idx, batch in enumerate(batches, 1)
            }

            for future in as_completed(futures):
                process_id = futures[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                except Exception as e:
                    print(f"\n✗ 进程{process_id}发生错误: {str(e)}")

        print("\n" + "=" * 70)

    except KeyboardInterrupt:
        print("\n\n⚠ 用户中断程序")
        return
    except Exception as e:
        print(f"\n✗ 发生严重错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return

    # 写入结果
    print("\n💾 正在写入结果...")

    # 按index排序
    all_results.sort(key=lambda x: x[1])

    success_count = 0
    failed_items = []

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        for success, index, line_num, id_value, result_dict, error_msg in all_results:
            if success:
                output_line = json.dumps(result_dict, ensure_ascii=False, separators=(',', ':'))
                outfile.write(output_line + '\n')
                success_count += 1
            else:
                failed_items.append({
                    'line_num': line_num,
                    'id': id_value,
                    'error': error_msg
                })

    # 统计信息
    elapsed_time = time.time() - start_time
    error_count = len(failed_items)

    print("\n" + "=" * 70)
    print("✅ 处理完成！")
    print("=" * 70)
    print(f"✓ 成功: {success_count} 条")
    print(f"✗ 失败: {error_count} 条")
    print(f"⏱ 总耗时: {elapsed_time:.2f} 秒")
    if len(tasks) > 0:
        print(f"⚡ 平均速度: {len(tasks) / elapsed_time:.2f} 条/秒")
    print(f"📁 输出文件: {OUTPUT_FILE}")
    print("=" * 70)

    # 打印失败的行
    if failed_items:
        print("\n" + "=" * 70)
        print(f"❌ 失败的任务详情 (共 {len(failed_items)} 条):")
        print("=" * 70)
        for item in failed_items:
            print(f"\n行号: {item['line_num']}")
            print(f"ID: {item['id']}")
            print(f"错误: {item['error']}")
            print("-" * 70)
        print("=" * 70)
    else:
        print("\n🎉 所有任务均处理成功！")


if __name__ == "__main__":
    main()