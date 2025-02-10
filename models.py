from sqlalchemy import create_engine, Column, String, Text, DateTime, Index, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class MigratedUser(Base):
    __tablename__ = 'migrated_users'
    user_id = Column(String, primary_key=True)
    processed_at = Column(DateTime, default=datetime.utcnow)

class ProcessedUser(Base):
    __tablename__ = 'processed_users'
    
    user_id = Column(String, primary_key=True)
    processed_at = Column(DateTime, default=datetime.utcnow)

class Message(Base):
    __tablename__ = 'messages'
    __table_args__ = (
        Index('idx_user_conversation', 'userId', 'conversationId'),
        Index('idx_created_at', 'createdAt')
    )

    id = Column(String, primary_key=True)
    promptId = Column(String)
    content = Column(Text)
    createdAt = Column(String)
    role = Column(String)
    type = Column(String)
    conversationId = Column(String)
    userId = Column(String)
    processed = Column(Boolean, default=False)

    def to_dict(self):
        return {
            'id': self.id,
            'promptId': self.promptId,
            'content': self.content,
            'createdAt': self.createdAt,
            'role': self.role,
            'type': self.type,
            'conversationId': self.conversationId
        }