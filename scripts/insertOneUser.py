import sys
import os
import json
from datetime import datetime
from typing import Dict

# 添加父目录到Python路径以导入项目模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_manager import DatabaseManager

def process_user_backup(file_path: str, user_id: str) -> bool:
    """处理单个用户的备份文件

    Args:
        file_path (str): JSON备份文件的路径
        user_id (str): 用户ID

    Returns:
        bool: 处理是否成功
    """
    if not os.path.exists(file_path):
        print(f"错误: 文件 {file_path} 不存在")
        return False

    try:
        db_manager = DatabaseManager()
        
        # 检查用户是否已经处理过
        if db_manager.is_user_processed(user_id):
            print(f"警告: 用户 {user_id} 已经处理过，是否继续? (y/n)")
            response = input().lower()
            if response != 'y':
                print("操作已取消")
                return False

        # 处理备份文件
        success = db_manager.process_backup_file(file_path, user_id)
        
        if success:
            # 标记用户为已处理
            db_manager.mark_user_as_processed(user_id)
            print(f"用户 {user_id} 的数据已成功导入")
        else:
            print(f"处理用户 {user_id} 的数据时发生错误")
        
        return success

    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        return False

def main():
    if len(sys.argv) != 3:
        print("使用方法: python insertOneUser.py <json文件路径> <用户ID>")
        sys.exit(1)

    file_path = sys.argv[1]
    user_id = sys.argv[2]

    success = process_user_backup(file_path, user_id)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()