# db.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float, JSON, Index, Text, Boolean
from datetime import datetime

DATABASE_URL = "sqlite+aiosqlite:///./db.sqlite3"

engine = create_async_engine(
    DATABASE_URL, 
    echo=False, 
    connect_args={"check_same_thread": False} 
)

SessionLocal = sessionmaker(
    bind=engine, 
    class_=AsyncSession, 
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), index=True, nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    role = Column(String(20), index=True, nullable=False, default="student")
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    
    created_courses = relationship('Course', back_populates='teacher', foreign_keys='Course.teacher_id')
    activities = relationship('Activity', back_populates='user', cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', email='{self.email}', role='{self.role}')>"

class Course(Base):
    __tablename__ = 'courses'
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(200), index=True, nullable=False)
    description = Column(Text, nullable=True)
    category = Column(String(50), index=True, nullable=False)
    level = Column(String(20), index=True, nullable=False)
    tags = Column(String(255), nullable=True, index=True) # <--- ДОБАВЛЕНО ПОЛЕ TAGS
    teacher_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    teacher = relationship('User', back_populates='created_courses')
    materials = relationship('Material', back_populates='course', cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_course_title_search', 'title'),
        Index('idx_course_category_level', 'category', 'level'),
        Index('idx_course_tags', 'tags'), # <--- ИНДЕКС ДЛЯ ТЕГОВ (опционально для БД, Whoosh будет основным)
    )

    def __repr__(self):
        return f"<Course(id={self.id}, title='{self.title}')>"

class Material(Base):
    __tablename__ = 'materials'
    
    id = Column(Integer, primary_key=True, index=True)
    course_id = Column(Integer, ForeignKey('courses.id', ondelete="CASCADE"), nullable=False)
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=True)
    type = Column(String(20), index=True, nullable=False)
    order_index = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    course = relationship('Course', back_populates='materials')
    activities = relationship('Activity', back_populates='material', cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Material(id={self.id}, title='{self.title}', type='{self.type}')>"

class Activity(Base):
    __tablename__ = 'activities'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete="CASCADE"), nullable=False)
    material_id = Column(Integer, ForeignKey('materials.id', ondelete="CASCADE"), nullable=False)
    action = Column(String(50), index=True, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)
    duration = Column(Float, nullable=True)
    score = Column(Float, nullable=True)
    meta = Column(JSON, nullable=True)
    
    user = relationship('User', back_populates='activities')
    material = relationship('Material', back_populates='activities')
    
    __table_args__ = (
        Index('idx_activity_user_material_action', 'user_id', 'material_id', 'action'),
    )

    def __repr__(self):
        return f"<Activity(id={self.id}, user_id={self.user_id}, action='{self.action}')>"