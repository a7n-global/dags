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
        """提交工作流"""
        url = f"{self.server_url}/api/v1/workflows/{self.namespace}"
        
        payload = {
            "workflow": {
                "metadata": {
                    "generateName": "deepseek-eval-api-",
                    "namespace": self.namespace
                },
                "spec": {
                    "workflowTemplateRef": {
                        "name": "deepseek-eval-template"
                    },
                    "arguments": {
                        "parameters": [
                            {"name": "job_name", "value": job_name},
                            {"name": "project_name", "value": "deepseek_v2_lite"},
                            {"name": "model_input", "value": model_input},
                            {"name": "task_input", "value": json.dumps(task_input)}
                        ]
                    }
                }
            }
        }
        
        response = requests.post(url, json=payload, verify=False)
        
        if response.status_code == 200:
            workflow_info = response.json()
            workflow_name = workflow_info['metadata']['name']
            print(f"✅ 工作流提交成功: {workflow_name}")
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

def parse_task_input(task_input_str):
    """解析任务输入，支持多种格式"""
    if not task_input_str:
        return []
    
    # 如果是JSON格式，直接解析
    if task_input_str.startswith('[') and task_input_str.endswith(']'):
        try:
            return json.loads(task_input_str)
        except json.JSONDecodeError:
            print(f"❌ 无效的JSON格式: {task_input_str}")
            sys.exit(1)
    
    # 如果是逗号分隔的字符串
    if ',' in task_input_str:
        return [task.strip() for task in task_input_str.split(',')]
    
    # 单个任务
    return [task_input_str.strip()]

def main():
    parser = argparse.ArgumentParser(description='Argo Workflows API 客户端')
    
    # 基础参数
    parser.add_argument('--server', default=None, 
                       help='Argo Server URL (默认: 自动检测 Pod内部/外部环境)')
    
    # 提交工作流参数
    parser.add_argument('--model_input', type=str,
                       help='模型输入路径，例如: /models/deepseek-v2')
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
    
    # 查看工作流日志
    if args.logs:
        client.get_workflow_logs(args.logs)
        return
    
    # 提交工作流
    if args.model_input and args.task_input:
        # 解析任务输入
        task_input = parse_task_input(args.task_input)
        
        # 生成任务名称
        if not args.job_name:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            args.job_name = f"eval-{timestamp}"
        
        print(f"📝 任务参数:")
        print(f"  任务名称: {args.job_name}")
        print(f"  模型路径: {args.model_input}")
        print(f"  评估任务: {task_input}")
        print(f"  项目名称: {args.project_name}")
        print()
        
        # 提交工作流
        workflow_name = client.submit_workflow(
            job_name=args.job_name,
            model_input=args.model_input,
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
        print("  # 提交工作流")
        print("  python3 argo_api_client.py --model_input /models/deepseek-v2 --task_input 'mmlu,hellaswag'")
        print("  python3 argo_api_client.py --model_input /models/deepseek-v2 --task_input '[\"mmlu\", \"hellaswag\"]' --watch")
        print("  # 管理工作流")
        print("  python3 argo_api_client.py --list")
        print("  python3 argo_api_client.py --status workflow-name")
        print("  python3 argo_api_client.py --logs workflow-name")

if __name__ == "__main__":
    main() 