#!/usr/bin/env python3
"""
Argo Workflows API 客户端
使用 requests 提交和管理工作流

用法示例:
python3 argo_api_client.py --model_input /models/deepseek-v2 --task_input '["mmlu", "hellaswag"]'
python3 argo_api_client.py --model_input /models/deepseek-v2 --task_input mmlu,hellaswag --job_name my-test
python3 argo_api_client.py --list  # 列出所有工作流
python3 argo_api_client.py --status workflow-name  # 查看工作流状态
"""

import requests
import json
import time
import urllib3
import argparse
import sys
import os
from pathlib import Path
from rich.console import Console
from rich.table import Table, box
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from datetime import datetime

# 忽略SSL证书警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ArgoWorkflowsClient:
    def __init__(self, server_url=None):
        # 自动检测运行环境
        if server_url is None:
            # 检查是否在 Kubernetes Pod 内部
            if os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount'):
                # Pod 内部，使用内部服务名
                self.server_url = "https://argo-server.argo.svc.cluster.local:2746"
                print("🔍 检测到运行在 Kubernetes Pod 内，使用内部服务地址")
            else:
                # Pod 外部，使用外部 LoadBalancer IP
                self.server_url = "https://10.218.61.160"
                print("🔍 检测到运行在 Pod 外部，使用 LoadBalancer 地址")
        else:
            self.server_url = server_url
            
        self.namespace = "argo"
        print(f"🌐 Argo Server URL: {self.server_url}")
        
    def submit_workflow(self, job_name, model_input, task_input):
        """提交多模型嵌套工作流"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}"
        
        # 为模型添加索引信息
        model_list_with_index = []
        for i, model_path in enumerate(model_input):
            model_list_with_index.append({
                "path": model_path,
                "index": i
            })
        
        payload = {
            "workflow": {
                "metadata": {
                    "generateName": "multi-eval-api-",
                    "namespace": self.namespace
                },
                "spec": {
                    "workflowTemplateRef": {
                        "name": "deepseek-multi-model-eval-template"
                    },
                    "arguments": {
                        "parameters": [
                            {"name": "job_name", "value": job_name},
                            {"name": "project_name", "value": "deepseek_v2_lite"},
                            {"name": "model_list", "value": json.dumps(model_list_with_index)},
                            {"name": "task_list", "value": json.dumps(task_input)}
                        ]
                    }
                }
            }
        }
        
        response = requests.post(url, json=payload, verify=False)
        
        if response.status_code == 200:
            workflow_info = response.json()
            workflow_name = workflow_info['metadata']['name']
            print(f"✅ 嵌套工作流提交成功: {workflow_name}")
            print(f"   🏭 模型流水线: {len(model_input)} 个")
            print(f"   📊 每个模型的任务: {len(task_input)} 个")
            print(f"   🎯 总评估任务: {len(model_input) * len(task_input)} 个")
            return workflow_name
        else:
            print(f"❌ 提交失败: {response.status_code} - {response.text}")
            return None
    
    def get_workflow_status(self, workflow_name):
        """获取工作流状态"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}/{workflow_name}"
        
        response = requests.get(url, verify=False)
        
        if response.status_code == 200:
            workflow = response.json()
            status = workflow['status']['phase']
            print(f"工作流 {workflow_name} 状态: {status}")
            return workflow
        else:
            print(f"获取状态失败: {response.status_code}")
            return None
    
    def list_workflows(self):
        """列出所有工作流"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}"
        
        try:
            response = requests.get(url, verify=False, timeout=30)
            
            print(f"🔍 请求 URL: {url}")
            print(f"📡 响应状态码: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    workflows = response.json()
                    if workflows is None:
                        print("❌ API 返回了空响应")
                        return None
                        
                    items = workflows.get('items', [])
                    if not items:
                        print("📝 暂无工作流")
                        return workflows
                        
                    print("工作流列表:")
                    for wf in items:
                        name = wf['metadata']['name']
                        status = wf['status']['phase']
                        created = wf['metadata'].get('creationTimestamp', 'N/A')
                        print(f"  {name}: {status} (创建时间: {created})")
                    return workflows
                except json.JSONDecodeError as e:
                    print(f"❌ JSON 解析失败: {e}")
                    print(f"🔍 响应内容: {response.text[:500]}...")
                    return None
            else:
                print(f"❌ 获取列表失败: {response.status_code}")
                print(f"🔍 错误内容: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ 网络请求失败: {e}")
            print(f"🔍 请检查 Argo Server 是否可访问: {self.server_url}")
            return None
        except Exception as e:
            print(f"❌ 未知错误: {e}")
            return None
    
    def get_workflow_logs(self, workflow_name):
        """获取工作流日志"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}/{workflow_name}/log"
        
        response = requests.get(url, verify=False)
        
        if response.status_code == 200:
            print(f"工作流 {workflow_name} 日志:")
            print(response.text)
            return response.text
        else:
            print(f"获取日志失败: {response.status_code}")
            return None
    
    def get_workflow_tasks(self, workflow_name):
        """获取工作流中每个任务的详细状态"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}/{workflow_name}"
        
        try:
            response = requests.get(url, verify=False, timeout=30)
            
            if response.status_code == 200:
                workflow = response.json()
                
                print(f"📊 工作流 {workflow_name} 任务详情:")
                print("=" * 60)
                
                # 获取总体状态
                overall_status = workflow['status']['phase']
                start_time = workflow['metadata'].get('creationTimestamp', 'N/A')
                print(f"🎯 总体状态: {overall_status}")
                print(f"⏰ 开始时间: {start_time}")
                print()
                
                # 获取所有节点（任务）状态
                nodes = workflow['status'].get('nodes', {})
                
                if not nodes:
                    print("❌ 未找到任务节点信息")
                    return None
                
                # 按照任务类型分组显示
                convert_tasks = []
                eval_tasks = []
                other_tasks = []
                
                for node_id, node in nodes.items():
                    node_name = node.get('displayName', node.get('name', 'unknown'))
                    phase = node.get('phase', 'Unknown')
                    start_time = node.get('startedAt', 'N/A')
                    finish_time = node.get('finishedAt', 'N/A')
                    
                    task_info = {
                        'name': node_name,
                        'phase': phase,
                        'start': start_time,
                        'finish': finish_time,
                        'node_id': node_id
                    }
                    
                    # 识别嵌套工作流的任务类型
                    if ('convert' in node_name.lower() or 
                        node_name.startswith('convert-models') or
                        'convert-model' in node_name.lower()):
                        # 转换任务（支持单模型、多模型和嵌套工作流）
                        convert_tasks.append(task_info)
                    elif ('run-eval' in node_name.lower() or 
                          'eval-tasks' in node_name.lower() or
                          node_name.startswith('eval-combinations') or
                          'run-single-eval' in node_name.lower()):
                        # 评估任务（放宽条件，支持嵌套工作流）
                        eval_tasks.append(task_info)
                    else:
                        other_tasks.append(task_info)
                
                # 显示转换任务
                if convert_tasks:
                    print("🔄 模型转换任务:")
                    for task in convert_tasks:
                        status_icon = self._get_status_icon(task['phase'])
                        print(f"  {status_icon} {task['name']}: {task['phase']}")
                        if task['start'] != 'N/A':
                            print(f"     开始: {task['start']}")
                        if task['finish'] != 'N/A':
                            print(f"     结束: {task['finish']}")
                        print()
                
                # 显示评估任务
                if eval_tasks:
                    print("📊 评估任务:")
                    for task in eval_tasks:
                        status_icon = self._get_status_icon(task['phase'])
                        print(f"  {status_icon} {task['name']}: {task['phase']}")
                        if task['start'] != 'N/A':
                            print(f"     开始: {task['start']}")
                        if task['finish'] != 'N/A':
                            print(f"     结束: {task['finish']}")
                        print()
                
                # 显示其他任务
                if other_tasks:
                    print("🔧 其他任务:")
                    for task in other_tasks:
                        status_icon = self._get_status_icon(task['phase'])
                        print(f"  {status_icon} {task['name']}: {task['phase']}")
                        if task['start'] != 'N/A':
                            print(f"     开始: {task['start']}")
                        if task['finish'] != 'N/A':
                            print(f"     结束: {task['finish']}")
                        print()
                
                return workflow
            else:
                print(f"❌ 获取任务状态失败: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ 获取任务状态时出错: {e}")
            return None
    
    def get_task_logs(self, workflow_name, task_name):
        """获取特定任务的日志"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}/{workflow_name}/{task_name}/log"
        
        try:
            response = requests.get(url, verify=False, timeout=30)
            
            if response.status_code == 200:
                print(f"📋 任务 {task_name} 日志:")
                print("-" * 40)
                print(response.text)
                return response.text
            else:
                print(f"❌ 获取任务 {task_name} 日志失败: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ 获取任务日志时出错: {e}")
            return None
    
    def get_workflow_tasks_detailed(self, workflow_name, show_logs=False):
        """获取工作流中每个任务的详细状态，支持显示日志"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}/{workflow_name}"
        
        try:
            response = requests.get(url, verify=False, timeout=30)
            
            if response.status_code == 200:
                workflow = response.json()
                
                print(f"📊 工作流 {workflow_name} 详细任务状态:")
                print("=" * 80)
                
                # 获取总体状态
                overall_status = workflow['status']['phase']
                start_time = workflow['metadata'].get('creationTimestamp', 'N/A')
                print(f"🎯 总体状态: {overall_status}")
                print(f"⏰ 开始时间: {start_time}")
                print()
                
                # 获取所有节点（任务）状态
                nodes = workflow['status'].get('nodes', {})
                
                if not nodes:
                    print("❌ 未找到任务节点信息")
                    return None
                
                # 按照执行顺序排序并分类显示
                convert_tasks = []
                eval_tasks = []
                other_tasks = []
                
                for node_id, node in nodes.items():
                    node_name = node.get('displayName', node.get('name', 'unknown'))
                    phase = node.get('phase', 'Unknown')
                    start_time = node.get('startedAt', 'N/A')
                    finish_time = node.get('finishedAt', 'N/A')
                    pod_name = node.get('id', node_id)  # 实际的 pod 名称
                    
                    task_info = {
                        'name': node_name,
                        'phase': phase,
                        'start': start_time,
                        'finish': finish_time,
                        'node_id': node_id,
                        'pod_name': pod_name
                    }
                    
                    # 只保留有意义的任务
                    if 'convert' in node_name.lower() or node_name.startswith('convert-models'):
                        # 转换任务（支持单模型和多模型工作流）
                        convert_tasks.append(task_info)
                    elif ('run-eval' in node_name.lower() or 
                          'eval-tasks' in node_name.lower() or
                          node_name.startswith('eval-combinations') or
                          'run-single-eval' in node_name.lower()):
                        # 评估任务（放宽条件，支持嵌套工作流）
                        eval_tasks.append(task_info)
                    else:
                        other_tasks.append(task_info)
                
                # 显示转换任务
                if convert_tasks:
                    print("🔄 模型转换任务:")
                    for task in convert_tasks:
                        self._display_task_details(task, workflow_name, show_logs)
                
                # 显示评估任务
                if eval_tasks:
                    print("📊 评估任务:")
                    for i, task in enumerate(eval_tasks):
                        print(f"  📋 子任务 {i+1}:")
                        self._display_task_details(task, workflow_name, show_logs, indent="    ")
                
                # 显示其他任务
                if other_tasks:
                    print("🔧 其他任务:")
                    for task in other_tasks:
                        self._display_task_details(task, workflow_name, show_logs)
                
                return workflow
            else:
                print(f"❌ 获取任务状态失败: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ 获取任务状态时出错: {e}")
            return None
    
    def _display_task_details(self, task, workflow_name, show_logs=False, indent="  "):
        """显示单个任务的详细信息"""
        status_icon = self._get_status_icon(task['phase'])
        print(f"{indent}{status_icon} {task['name']}: {task['phase']}")
        print(f"{indent}   Pod: {task['pod_name']}")
        
        if task['start'] != 'N/A':
            print(f"{indent}   开始: {task['start']}")
        if task['finish'] != 'N/A':
            print(f"{indent}   结束: {task['finish']}")
        
        # 如果要显示日志且任务正在运行或已完成
        if show_logs and task['phase'] in ['Running', 'Succeeded', 'Failed']:
            print(f"{indent}   📋 任务日志:")
            print(f"{indent}   " + "-" * 40)
            try:
                logs = self.get_task_logs(workflow_name, task['pod_name'])
                if logs:
                    # 只显示最后几行日志，避免输出过长
                    log_lines = logs.strip().split('\n')
                    if len(log_lines) > 10:
                        print(f"{indent}   ... (省略前面的日志)")
                        for line in log_lines[-10:]:
                            print(f"{indent}   {line}")
                    else:
                        for line in log_lines:
                            print(f"{indent}   {line}")
                    print(f"{indent}   " + "-" * 40)
            except Exception as e:
                print(f"{indent}   ❌ 获取日志失败: {e}")
        
        print()
    
    def _get_status_icon(self, phase):
        """根据任务状态返回对应的图标"""
        status_icons = {
            'Pending': '⏳',
            'Running': '🔄',
            'Succeeded': '✅',
            'Failed': '❌',
            'Error': '💥',
            'Skipped': '⏭️',
            'Omitted': '⚪'
        }
        return status_icons.get(phase, '❓')
    
    def display_workflow_tasks_table(self, workflow_name):
        """使用 Rich 表格显示工作流任务状态"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}/{workflow_name}"
        
        try:
            response = requests.get(url, verify=False, timeout=30)
            
            if response.status_code == 200:
                workflow = response.json()
                
                # 创建 Rich 控制台
                console = Console(width=150)
                
                # 获取总体状态信息
                overall_status = workflow['status']['phase']
                start_time = workflow['metadata'].get('creationTimestamp', 'N/A')
                
                # 显示工作流基本信息
                workflow_info = Table(title=f"🚀 Workflow: {workflow_name}", box=box.ROUNDED)
                workflow_info.add_column("属性", style="cyan", width=20)
                workflow_info.add_column("值", style="green")
                
                workflow_info.add_row("总体状态", self._get_status_text(overall_status))
                workflow_info.add_row("开始时间", start_time)
                
                # 计算运行时长
                if start_time != 'N/A':
                    try:
                        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        now = datetime.now(start_dt.tzinfo)
                        duration = str(now - start_dt).split('.')[0]  # 去掉微秒
                        workflow_info.add_row("运行时长", duration)
                    except:
                        workflow_info.add_row("运行时长", "计算失败")
                
                console.print(workflow_info)
                console.print()
                
                # 获取所有节点（任务）状态
                nodes = workflow['status'].get('nodes', {})
                
                if not nodes:
                    console.print("[red]❌ 未找到任务节点信息[/red]")
                    return None
                
                # 创建任务状态表格
                tasks_table = Table(title="📋 任务执行状态", box=box.ROUNDED, show_lines=True)
                tasks_table.add_column("任务类型", style="magenta", width=15)
                tasks_table.add_column("任务名称", style="blue", width=40, no_wrap=False)
                tasks_table.add_column("状态", style="bold", width=12)
                tasks_table.add_column("开始时间", style="yellow", width=20)
                tasks_table.add_column("结束时间", style="yellow", width=20)
                tasks_table.add_column("耗时", style="green", width=15)
                tasks_table.add_column("Pod名称", style="dim", width=25, no_wrap=True)
                
                # 分类和处理任务
                convert_tasks = []
                eval_tasks = []
                other_tasks = []
                
                for node_id, node in nodes.items():
                    node_name = node.get('displayName', node.get('name', 'unknown'))
                    phase = node.get('phase', 'Unknown')
                    start_time = node.get('startedAt', 'N/A')
                    finish_time = node.get('finishedAt', 'N/A')
                    pod_name = node.get('id', node_id)
                    
                    # 计算耗时
                    duration = self._calculate_duration(start_time, finish_time, phase)
                    
                    task_info = {
                        'name': node_name,
                        'phase': phase,
                        'start': start_time,
                        'finish': finish_time,
                        'duration': duration,
                        'pod_name': pod_name
                    }
                    
                    # 只保留有意义的任务
                    if 'convert' in node_name.lower() or node_name.startswith('convert-models'):
                        # 转换任务（支持单模型和多模型工作流）
                        convert_tasks.append(task_info)
                    elif ('run-eval' in node_name.lower() or 
                          'eval-tasks' in node_name.lower() or
                          node_name.startswith('eval-combinations') or
                          'run-single-eval' in node_name.lower()):
                        # 评估任务（放宽条件，支持嵌套工作流）
                        eval_tasks.append(task_info)
                
                # 添加任务到表格
                # 转换任务
                for task in convert_tasks:
                    tasks_table.add_row(
                        "🔄 转换",
                        task['name'],
                        self._get_status_text(task['phase']),
                        self._format_time(task['start']),
                        self._format_time(task['finish']),
                        task['duration'],
                        task['pod_name']
                    )
                
                # 评估任务
                for task in eval_tasks:
                    # 从任务名称中提取评估内容
                    task_type = "📊 评估"
                    if ':' in task['name']:
                        eval_content = task['name'].split(':', 1)[1].strip(')')
                        # 截取评估内容用于显示
                        if len(eval_content) > 20:
                            task_type = f"📊 {eval_content[:20]}..."
                        else:
                            task_type = f"📊 {eval_content}"
                    
                    tasks_table.add_row(
                        task_type,
                        task['name'],
                        self._get_status_text(task['phase']),
                        self._format_time(task['start']),
                        self._format_time(task['finish']),
                        task['duration'],
                        task['pod_name']
                    )
                
                console.print(tasks_table)
                
                # 显示统计信息
                stats_table = Table(title="📈 任务统计", box=box.ROUNDED)
                stats_table.add_column("状态", style="cyan")
                stats_table.add_column("数量", style="green")
                
                all_tasks = convert_tasks + eval_tasks  # 只统计有意义的任务
                status_counts = {}
                for task in all_tasks:
                    status = task['phase']
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                for status, count in status_counts.items():
                    stats_table.add_row(self._get_status_text(status), str(count))
                
                console.print(stats_table)
                
                return workflow
            else:
                console = Console()
                console.print(f"[red]❌ 获取任务状态失败: {response.status_code}[/red]")
                return None
                
        except Exception as e:
            console = Console()
            console.print(f"[red]❌ 获取任务状态时出错: {e}[/red]")
            return None
    
    def _get_status_text(self, phase):
        """根据状态返回带颜色的文本"""
        status_colors = {
            'Pending': '[yellow]⏳ Pending[/yellow]',
            'Running': '[blue]🔄 Running[/blue]',
            'Succeeded': '[green]✅ Succeeded[/green]',
            'Failed': '[red]❌ Failed[/red]',
            'Error': '[red]💥 Error[/red]',
            'Skipped': '[dim]⏭️ Skipped[/dim]',
            'Omitted': '[dim]⚪ Omitted[/dim]'
        }
        return status_colors.get(phase, f'[dim]❓ {phase}[/dim]')
    
    def _format_time(self, time_str):
        """格式化时间显示"""
        if time_str == 'N/A' or not time_str:
            return '-'
        try:
            # 解析ISO格式时间并格式化
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return dt.strftime('%H:%M:%S')
        except:
            return time_str[:19] if len(time_str) > 19 else time_str
    
    def _calculate_duration(self, start_time, finish_time, phase):
        """计算任务耗时"""
        if start_time == 'N/A' or not start_time:
            return '-'
        
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            
            if finish_time and finish_time != 'N/A':
                # 已完成的任务
                finish_dt = datetime.fromisoformat(finish_time.replace('Z', '+00:00'))
                duration = finish_dt - start_dt
            elif phase == 'Running':
                # 正在运行的任务
                now = datetime.now(start_dt.tzinfo)
                duration = now - start_dt
            else:
                return '-'
            
            # 格式化耗时显示
            total_seconds = int(duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            if hours > 0:
                return f"{hours}h{minutes}m{seconds}s"
            elif minutes > 0:
                return f"{minutes}m{seconds}s"
            else:
                return f"{seconds}s"
                
        except Exception as e:
            return '-'

def discover_model_directories(base_path):
    """
    自动发现指定路径下的所有模型目录
    
    参数：
    - base_path: 基础路径，如 "/models"
    
    返回：
    - 模型目录列表，如 ["/models/deepseek-v2", "/models/qwen-2"]
    """
    try:
        base_path = Path(base_path)
        
        if not base_path.exists():
            print(f"❌ 路径不存在: {base_path}")
            return []
            
        if not base_path.is_dir():
            print(f"❌ 不是目录: {base_path}")
            return []
        
        # 获取所有子目录
        model_dirs = []
        for item in base_path.iterdir():
            if item.is_dir():
                # 过滤掉隐藏目录和常见的非模型目录
                if not item.name.startswith('.') and item.name not in ['__pycache__', 'tmp', 'cache']:
                    model_dirs.append(str(item.absolute()))
        
        if not model_dirs:
            print(f"⚠️ 在 {base_path} 下未找到任何子目录")
            return []
        
        # 按名称排序
        model_dirs.sort()
        
        print(f"🔍 在 {base_path} 下发现 {len(model_dirs)} 个模型目录:")
        for i, model_dir in enumerate(model_dirs):
            model_name = Path(model_dir).name
            print(f"  {i+1}. {model_name} ({model_dir})")
        
        return model_dirs
        
    except Exception as e:
        print(f"❌ 扫描目录时出错: {e}")
        return []

def parse_model_input(model_input_str):
    """
    解析模型输入，支持多种格式
    
    格式说明：
    - 单个路径（目录）："/models" → 自动发现子目录
    - 单个模型："/models/deepseek-v2"
    - 多个模型（逗号分隔）："/models/deepseek-v2,/models/qwen-2"
    - JSON数组：'["/models/deepseek-v2", "/models/qwen-2"]'
    """
    if not model_input_str:
        return []
    
    # 如果是JSON格式，直接解析
    if model_input_str.startswith('[') and model_input_str.endswith(']'):
        try:
            return json.loads(model_input_str)
        except json.JSONDecodeError:
            print(f"❌ 无效的JSON格式: {model_input_str}")
            sys.exit(1)
    
    # 按逗号分隔多个模型（向后兼容）
    if ',' in model_input_str:
        models = [model.strip() for model in model_input_str.split(',')]
        print(f"🔍 检测到多个模型: {models}")
        return models
    
    # 单个路径 - 检查是否为目录
    model_path = model_input_str.strip()
    if os.path.isdir(model_path):
        print(f"🔍 检测到目录，自动扫描子目录: {model_path}")
        return discover_model_directories(model_path)
    else:
        # 单个模型文件/目录
        if os.path.exists(model_path):
            print(f"🔍 单个模型: {model_path}")
            return [model_path]
        else:
            print(f"⚠️ 路径不存在，但继续处理: {model_path}")
            return [model_path]

def generate_model_task_combinations(model_list, task_list):
    """
    生成模型和任务的所有组合
    
    返回：
    - convert_jobs: [{"model": "/path/to/model", "index": 0}, ...]
    - eval_jobs: [{"model": "/path/to/model", "task": "task_name", "model_index": 0, "task_index": 0}, ...]
    """
    convert_jobs = []
    eval_jobs = []
    
    for model_idx, model_path in enumerate(model_list):
        # 转换任务（使用 "index" 而不是 "model_index" 来匹配 WorkflowTemplate）
        convert_jobs.append({
            "model": model_path,
            "index": model_idx
        })
        
        # 评估任务
        for task_idx, task in enumerate(task_list):
            eval_jobs.append({
                "model": model_path,
                "task": task,
                "model_index": model_idx,
                "task_index": task_idx
            })
    
    return convert_jobs, eval_jobs

def parse_task_input(task_input_str):
    """
    解析任务输入，支持多种格式
    
    格式说明：
    - 逗号分隔：合并为一个任务，如 "hellaswag,truthfulqa" → ["hellaswag truthfulqa"]
    - 分号分隔：创建多个任务，如 "hellaswag,truthfulqa;a,b,c" → ["hellaswag truthfulqa", "a b c"]
    - JSON数组：直接解析，如 '["task1", "task2"]'
    """
    if not task_input_str:
        return []
    
    # 如果是JSON格式，直接解析
    if task_input_str.startswith('[') and task_input_str.endswith(']'):
        try:
            return json.loads(task_input_str)
        except json.JSONDecodeError:
            print(f"❌ 无效的JSON格式: {task_input_str}")
            sys.exit(1)
    
    # 按分号分隔任务组
    if ';' in task_input_str:
        task_groups = task_input_str.split(';')
        result = []
        for i, group in enumerate(task_groups):
            group = group.strip()
            if ',' in group:
                # 组内逗号分隔的子任务，合并为一个字符串
                subtasks = [task.strip() for task in group.split(',')]
                merged_task = ' '.join(subtasks)
                result.append(merged_task)
                print(f"🔍 任务组 {i+1}: {subtasks} → 合并为: '{merged_task}'")
            else:
                result.append(group)
                print(f"🔍 任务组 {i+1}: 单任务 '{group}'")
        return result
    
    # 如果只有逗号分隔，合并为一个任务
    if ',' in task_input_str:
        subtasks = [task.strip() for task in task_input_str.split(',')]
        merged_task = ' '.join(subtasks)
        print(f"🔍 检测到逗号分隔的子任务: {subtasks} → 合并为: '{merged_task}'")
        return [merged_task]
    
    # 单个任务
    return [task_input_str.strip()]

def main():
    parser = argparse.ArgumentParser(description='Argo Workflows API 客户端')
    
    # 基础参数
    parser.add_argument('--server', default=None, 
                       help='Argo Server URL (默认: 自动检测 Pod内部/外部环境)')
    
    # 提交工作流参数
    parser.add_argument('--model_input', type=str,
                       help='模型输入路径。支持: 1) 目录路径(自动发现子目录): "/models" 2) 单个模型: "/models/deepseek-v2" 3) 多个模型: "/models/m1,/models/m2"')
    parser.add_argument('--task_input', type=str,
                       help='评估任务列表，支持格式: "mmlu,hellaswag" 或 \'["mmlu", "hellaswag"]\'')
    parser.add_argument('--job_name', type=str,
                       help='任务名称 (默认: auto-generated)')
    parser.add_argument('--project_name', type=str, default='deepseek_v2_lite',
                       help='项目名称 (默认: deepseek_v2_lite)')
    
    # 管理参数
    parser.add_argument('--list', action='store_true',
                       help='列出所有工作流')
    parser.add_argument('--status', type=str,
                       help='查看指定工作流状态')
    parser.add_argument('--tasks', type=str,
                       help='查看指定工作流的所有任务详细状态')
    parser.add_argument('--table', type=str,
                       help='使用 Rich 表格显示指定工作流的任务状态')
    parser.add_argument('--tasks-with-logs', type=str,
                       help='查看指定工作流的所有任务状态并显示日志')
    parser.add_argument('--task-log', nargs=2, metavar=('WORKFLOW', 'TASK'),
                       help='查看指定工作流中特定任务的日志')
    parser.add_argument('--logs', type=str,
                       help='查看指定工作流日志')
    
    # 监控参数
    parser.add_argument('--watch', action='store_true',
                       help='提交后持续监控工作流状态直到完成')
    
    args = parser.parse_args()
    
    # 创建客户端
    client = ArgoWorkflowsClient(server_url=args.server)
    
    # 列出工作流
    if args.list:
        client.list_workflows()
        return
    
    # 查看工作流状态
    if args.status:
        client.get_workflow_status(args.status)
        return
    
    # 查看工作流任务详情
    if args.tasks:
        client.get_workflow_tasks_detailed(args.tasks)
        return
    
    # 查看工作流任务详情（Rich表格）
    if args.table:
        client.display_workflow_tasks_table(args.table)
        return
    
    # 查看工作流任务详情（带日志）
    if args.tasks_with_logs:
        client.get_workflow_tasks_detailed(args.tasks_with_logs, show_logs=True)
        return
    
    # 查看特定任务日志
    if args.task_log:
        workflow_name, task_name = args.task_log
        client.get_task_logs(workflow_name, task_name)
        return
    
    # 查看工作流日志
    if args.logs:
        client.get_workflow_logs(args.logs)
        return
    
    # 提交工作流
    if args.model_input and args.task_input:
        # 解析模型输入
        model_input = parse_model_input(args.model_input)
        if not model_input:
            print("❌ 模型输入不能为空")
            sys.exit(1)
            
        # 解析任务输入
        task_input = parse_task_input(args.task_input)
        if not task_input:
            print("❌ 任务输入不能为空")
            sys.exit(1)
        
        # 生成任务名称
        if not args.job_name:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            args.job_name = f"eval-{timestamp}"
        
        print(f"📝 嵌套工作流参数:")
        print(f"  任务名称: {args.job_name}")
        print(f"  模型列表: {model_input}")
        print(f"  评估任务: {task_input}")
        print(f"  项目名称: {args.project_name}")
        print()
        print(f"📊 执行计划（嵌套流水线）:")
        print(f"  🏭 模型流水线数: {len(model_input)}")
        print(f"  📊 每个模型的任务数: {len(task_input)}")
        print(f"  🎯 总评估任务数: {len(model_input) * len(task_input)}")
        print()
        
        # 显示详细的执行计划
        print("🏭 流水线详情:")
        for i, model_path in enumerate(model_input):
            model_name = model_path.split('/')[-1]
            print(f"  流水线 {i+1}: {model_name}")
            print(f"    🔄 转换: {model_path}")
            for j, task in enumerate(task_input):
                print(f"    📊 评估 {j+1}: {task}")
        print()
        
        # 提交工作流
        workflow_name = client.submit_workflow(
            job_name=args.job_name,
            model_input=model_input,
            task_input=task_input
        )
        
        if workflow_name and args.watch:
            print(f"🔍 监控工作流状态...")
            while True:
                workflow = client.get_workflow_status(workflow_name)
                if workflow:
                    status = workflow['status']['phase']
                    if status in ['Succeeded', 'Failed', 'Error']:
                        print(f"🏁 工作流完成，最终状态: {status}")
                        if status == 'Succeeded':
                            print("📋 获取日志:")
                            client.get_workflow_logs(workflow_name)
                        break
                print("⏳ 等待30秒后再检查...")
                time.sleep(30)
    
    else:
        print("❌ 请提供必要的参数来提交工作流，或使用管理命令")
        print("示例:")
        print("  # 提交工作流（新功能：自动发现模型目录）")
        print("  python3 argo_api_client.py --model_input /models --task_input 'mmlu,hellaswag'")
        print("  python3 argo_api_client.py --model_input /models --task_input 'wikitext,games;hellaswag,truthfulqa'")
        print("  # 提交工作流（传统方式）")
        print("  python3 argo_api_client.py --model_input /models/deepseek-v2 --task_input 'mmlu,hellaswag'")
        print("  python3 argo_api_client.py --model_input '/models/m1,/models/m2' --task_input '[\"mmlu\", \"hellaswag\"]' --watch")
        print("  # 管理工作流")
        print("  python3 argo_api_client.py --list")
        print("  python3 argo_api_client.py --status workflow-name")
        print("  python3 argo_api_client.py --tasks workflow-name")
        print("  python3 argo_api_client.py --table workflow-name  # 美化表格显示")
        print("  python3 argo_api_client.py --tasks-with-logs workflow-name")
        print("  python3 argo_api_client.py --task-log workflow-name task-name")
        print("  python3 argo_api_client.py --logs workflow-name")

if __name__ == "__main__":
    main() 