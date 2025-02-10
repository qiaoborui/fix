import json
import os
from datetime import datetime
from typing import List, Dict
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from models import Base, Message, ProcessedUser,MigratedUser

class DatabaseManager:
    def __init__(self, db_path: str = 'messages.db'):
        try:
            if os.path.exists(db_path):
                if not os.access(db_path, os.W_OK):
                    raise PermissionError(f"数据库文件 {db_path} 没有写入权限")
            
            self.engine = create_engine(f'sqlite:///{db_path}')
            self.Session = sessionmaker(bind=self.engine)
            self.init_database()
        except Exception as e:
            print(f"初始化数据库管理器时出错: {e}")
            raise
    
    def init_database(self):
        Base.metadata.create_all(self.engine)
    
    def insert_message(self, message: Dict, user_id: str):
        session = self.Session()
        try:
            db_message = Message(
                id=message['id'],
                promptId=message['promptId'],
                content=message['content'],
                createdAt=message['createdAt'],
                role=message['role'],
                type=message['type'],
                conversationId=message['conversationId'],
                userId=user_id
            )
            session.merge(db_message)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_user_conversations(self, user_id: str):
        session = self.Session()
        try:
            conversation_ids = session.query(Message.conversationId)\
                .filter(Message.userId == user_id)\
                .group_by(Message.conversationId)\
                .order_by(func.min(Message.createdAt))\
                .distinct()\
                .all()
            
            conversations = []
            for (conv_id,) in conversation_ids:
                messages = session.query(Message)\
                    .filter(Message.userId == user_id, Message.conversationId == conv_id)\
                    .order_by(Message.createdAt)\
                    .all()
                
                conversations.append({
                    'conversationId': conv_id,
                    'messages': [message.to_dict() for message in messages]
                })
            
            return conversations
        finally:
            session.close()

    def get_users(self,take: int, skip: int):
        session = self.Session()
        users = session.query(Message.userId)\
           .join(MigratedUser, Message.userId != MigratedUser.user_id, isouter=True)\
           .filter(MigratedUser.user_id == None)\
           .order_by(Message.createdAt.desc())\
           .limit(take)\
           .offset(skip)\
           .distinct()\
           .all()  
        return users 
           
    def is_user_processed(self, user_id: str) -> bool:
        session = self.Session()
        try:
            processed = session.query(ProcessedUser).filter(ProcessedUser.user_id == user_id).first()
            return processed is not None
        finally:
            session.close()

    def is_user_migrated(self, user_id: str) -> bool:
        session = self.Session()
        try:
            processed = session.query(MigratedUser).filter(MigratedUser.user_id == user_id).first()
            return processed is not None
        finally:
            session.close()

    def mark_user_as_migrated(self, user_id: str):
        session = self.Session()
        try:
            processed_user = MigratedUser(user_id=user_id)
            session.merge(processed_user)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def mark_user_as_processed(self, user_id: str):
        session = self.Session()
        try:
            processed_user = ProcessedUser(user_id=user_id)
            session.merge(processed_user)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def mark_conversation_as_processed(self, conversationId: str):
        session = self.Session()
        try:
            messages = session.query(Message)\
                .filter(Message.conversationId == conversationId)\
                .all()
            
            for message in messages:
                message.processed = True
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def process_backup_file(self, file_path: str, user_id: str):
        session = None
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    print(f"备份文件格式错误: {file_path}")
                    return False
                
                total_messages = len(data)
                print(f"开始处理 {total_messages} 条消息记录...")
                
                session = self.Session()
                batch_size = 5000  # 增加批处理大小以提高性能
                messages_batch = []
                last_progress_time = datetime.now()
                progress_interval = 2  # 每2秒更新一次进度
                
                for i, message in enumerate(data, 1):
                    try:
                        db_message = Message(
                            id=message['id'],
                            promptId=message['promptId'],
                            content=message['content'],
                            createdAt=message['createdAt'],
                            role=message['role'],
                            type=message['type'],
                            conversationId=message['conversationId'],
                            userId=user_id
                        )
                        messages_batch.append(db_message)
                        
                        current_time = datetime.now()
                        time_diff = (current_time - last_progress_time).total_seconds()
                        
                        # 批量提交条件：达到批处理大小、处理完所有数据，或者距离上次进度显示超过指定时间
                        if len(messages_batch) >= batch_size or i == total_messages or time_diff >= progress_interval:
                            if messages_batch:  # 确保有数据才进行提交
                                try:
                                    session.bulk_save_objects(messages_batch, preserve_order=False)
                                    session.commit()
                                except Exception as e:
                                    if 'UNIQUE constraint failed' in str(e):
                                        print(f"警告: 跳过重复的消息记录")
                                        session.rollback()
                                    else:
                                        print(f"处理消息时出错: {e}")
                                        session.rollback()
                                        return False
                                messages_batch = []
                            
                            # 显示进度
                            progress = (i / total_messages) * 100
                            print(f"处理进度: {progress:.1f}% ({i}/{total_messages}), 已处理 {i} 条记录")
                            last_progress_time = current_time
                            
                    except Exception as e:
                        print(f"处理消息时出错: {e}")
                        session.rollback()
                        return False
                
                print("数据处理完成！")
                return True
                
        except Exception as e:
            print(f"处理备份文件时出错: {e}")
            print(f"文件路径: {file_path}")
            if session:
                session.rollback()
            return False
        finally:
            if session:
                session.close()