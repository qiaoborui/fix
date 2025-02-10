import sqlite3
import os

def migrate_database():
    db_path = 'messages.db'
    
    if not os.path.exists(db_path):
        print("数据库文件不存在")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 检查 processed 列是否已存在
        cursor.execute("PRAGMA table_info(messages)")
        columns = cursor.fetchall()
        column_names = [column[1] for column in columns]
        
        if 'processed' not in column_names:
            print("添加 processed 列...")
            cursor.execute("ALTER TABLE messages ADD COLUMN processed BOOLEAN DEFAULT 0")
            conn.commit()
            print("成功添加 processed 列")
        else:
            print("processed 列已存在")
            
    except Exception as e:
        print(f"迁移过程中出错: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database() 