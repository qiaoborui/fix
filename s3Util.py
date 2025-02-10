import boto3
import os
from typing import List
import shutil
from db_manager import DatabaseManager

class S3BackupManager:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'flow-app-uploads-temp'
        self.base_prefix = 'app-user-messages/'
        self.download_base_dir = 'downloaded_backups'
        self.db_manager = DatabaseManager()

    def process_user_backups(self, user_id: str) -> bool:
        """下载并处理用户的备份文件"""
        # 检查用户是否已经处理过
        if self.db_manager.is_user_processed(user_id):
            print(f"用户 {user_id} 已经处理过，跳过处理")
            return True

        download_dir = self.download_user_backups(user_id)
        if not download_dir:
            return False

        success = True
        try:
            # 处理下载目录中的所有备份文件
            for file_name in os.listdir(download_dir):
                file_path = os.path.join(download_dir, file_name)
                if not self.db_manager.process_backup_file(file_path, user_id):
                    success = False
                    break

            # 处理完成后移动到已处理目录并标记用户为已处理
            if success:
                # self.move_user_directory(user_id, "processed-backups")
                self.db_manager.mark_user_as_processed(user_id)

            # 清理下载目录
            shutil.rmtree(download_dir)
            return success

        except Exception as e:
            print(f"处理用户备份时出错: {e}")
            return False

    def process_all_backups(self):
        """处理所有用户的备份文件，每个用户只处理最早的备份"""
        processed_count = 0
        for user_id, earliest_backup in self.backup_processor():
            # if processed_count >= 10:
            #     print("已处理10条记录，测试完成")
            #     break

            print(f"处理用户 {user_id} 的备份")
            if self.db_manager.is_user_processed(user_id):
                print(f"用户 {user_id} 已经处理过，跳过处理")
                continue

            # 创建临时下载目录
            user_download_dir = os.path.join(self.download_base_dir, user_id)
            os.makedirs(user_download_dir, exist_ok=True)

            try:
                # 只下载最早的备份文件
                file_name = os.path.basename(earliest_backup)
                download_path = os.path.join(user_download_dir, file_name)
                
                print(f"Downloading {earliest_backup} to {download_path}")
                self.s3_client.download_file(
                    self.bucket_name,
                    earliest_backup,
                    download_path
                )

                # 处理备份文件
                if self.db_manager.process_backup_file(download_path, user_id):
                    # 处理成功后移动文件并标记用户
                    self.move_user_directory(user_id, "processed-backups")
                    self.db_manager.mark_user_as_processed(user_id)
                    processed_count += 1
                    print(f"用户 {user_id} 的备份处理完成")
                else:
                    print(f"用户 {user_id} 的备份处理失败")

                # 清理下载目录
                shutil.rmtree(user_download_dir)

            except Exception as e:
                print(f"处理用户备份时出错: {e}")
                if os.path.exists(user_download_dir):
                    shutil.rmtree(user_download_dir)
                continue

    def list_user_backups(self, user_id: str) -> List[str]:
        prefix = f"{self.base_prefix}{user_id}/"
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )
        
        backup_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    backup_files.append(obj['Key'])
        
        return backup_files

    def download_user_backups(self, user_id: str) -> str:
        """下载指定用户的所有备份文件"""
        backup_files = self.list_user_backups(user_id)
        if not backup_files:
            print(f"No backup files found for user {user_id}")
            return ""

        # 创建下载目录
        user_download_dir = os.path.join(self.download_base_dir, user_id)
        os.makedirs(user_download_dir, exist_ok=True)

        # 下载所有文件
        for file_key in backup_files:
            file_name = os.path.basename(file_key)
            download_path = os.path.join(user_download_dir, file_name)
            
            print(f"Downloading {file_key} to {download_path}")
            self.s3_client.download_file(
                self.bucket_name,
                file_key,
                download_path
            )

        return user_download_dir

    def move_user_directory(self, user_id: str, destination: str) -> bool:
        """在 S3 上将用户的备份文件移动到新位置
        Args:
            user_id: 用户ID
            destination: 目标前缀，例如 'processed-backups'
        """
        source_prefix = f"{self.base_prefix}{user_id}/"
        backup_files = self.list_user_backups(user_id)
        
        if not backup_files:
            print(f"No files found for user {user_id}")
            return False

        try:
            for file_key in backup_files:
                # 构建新的目标路径
                file_name = os.path.basename(file_key)
                new_key = f"{destination}/{user_id}/{file_name}"
                
                # 复制文件到新位置
                print(f"Moving {file_key} to {new_key}")
                self.s3_client.copy_object(
                    Bucket=self.bucket_name,
                    CopySource={'Bucket': self.bucket_name, 'Key': file_key},
                    Key=new_key
                )
                
                # 删除原文件
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=file_key
                )
            
            print(f"Successfully moved all files for user {user_id} to {destination}")
            return True
            
        except Exception as e:
            print(f"Error moving files in S3: {e}")
            return False

    def list_user_ids(self) -> List[str]:
        """列出所有存在备份的用户ID"""
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=self.base_prefix,
            Delimiter='/'
        )
        
        user_ids = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                # 从路径中提取用户ID
                user_id = prefix['Prefix'].replace(self.base_prefix, '').rstrip('/')
                user_ids.append(user_id)
        
        return user_ids

    def backup_processor(self):
        """生成器：逐个处理用户的最早备份文件
        yields: (user_id, earliest_backup_file)
        """
        user_ids = self.list_user_ids()
        for user_id in user_ids:
            backup_files = self.list_user_backups(user_id)
            if not backup_files:
                continue
                
            # 通过文件名（时间戳）找出最早的备份
            earliest_backup = min(
                backup_files,
                key=lambda x: int(os.path.basename(x).replace('.json', ''))
            )
            yield user_id, earliest_backup