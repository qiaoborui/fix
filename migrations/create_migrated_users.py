from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime
from datetime import datetime
import os

def create_migrated_users_table():
    # 获取数据库路径
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'messages.db')
    
    # 创建数据库引擎
    engine = create_engine(f'sqlite:///{db_path}')
    
    # 创建 MetaData 实例
    metadata = MetaData()
    
    # 定义 migrated_users 表
    migrated_users = Table(
        'migrated_users',
        metadata,
        Column('user_id', String, primary_key=True),
        Column('processed_at', DateTime, default=datetime.utcnow)
    )
    
    try:
        # 创建表
        metadata.create_all(engine)
        print("成功创建 migrated_users 表")
    except Exception as e:
        print(f"创建表时发生错误: {e}")

if __name__ == '__main__':
    create_migrated_users_table()