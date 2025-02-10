import os
import time
from s3Util import S3BackupManager
from db_manager import DatabaseManager

def migrate():
    try:
        print("初始化S3备份管理器...")
        manager = S3BackupManager()
        
        while True:
            try:
                print("\n开始新一轮备份文件处理...")
                manager.process_all_backups()
                print("本轮处理完成，等待60秒后开始下一轮...")
                time.sleep(60)  # 休眠60秒后继续下一轮处理
                
            except Exception as e:
                print(f"处理过程中发生错误: {e}")
                print("等待60秒后重试...")
                time.sleep(60)
                continue
            
    except Exception as e:
        print(f"程序初始化过程中发生错误: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = migrate()
    exit(exit_code)
