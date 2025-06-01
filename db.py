# db.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float, JSON, Index, Text, Boolean # Добавил Boolean
from datetime import datetime

DATABASE_URL = "sqlite+aiosqlite:///./db.sqlite3" # Оставим SQLite для простоты примера

engine = create_async_engine(
    DATABASE_URL,
    echo=False, # В проде False, для отладки можно True
    connect_args={"check_same_thread": False} # Нужно для SQLite при работе с FastAPI/async
)

SessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False, # Явно указываем, что автокоммита нет
    autoflush=False
)

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), index=True, nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False) # Сделаем email обязательным
    role = Column(String(20), index=True, nullable=False, default="student") # default для роли
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False) # Используем Boolean
    
    # Связи (были courses, теперь created_courses для ясности, если User.courses будет для записанных курсов)
    created_courses = relationship('Course', back_populates='teacher', foreign_keys='Course.teacher_id')
    activities = relationship('Activity', back_populates='user', cascade="all, delete-orphan") # cascade добавлен

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', email='{self.email}', role='{self.role}')>"

class Course(Base):
    __tablename__ = 'courses'
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(200), index=True, nullable=False)
    description = Column(Text, nullable=True) # nullable=True если описание не обязательно
    category = Column(String(50), index=True, nullable=False)
    level = Column(String(20), index=True, nullable=False) # 'beginner', 'intermediate', 'advanced'
    teacher_id = Column(Integer, ForeignKey('users.id'), nullable=True) # Может быть курс без преподавателя? Если нет, то nullable=False
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    # is_active = Column(Boolean, default=True, nullable=False) # Если нужно поле активности курса

    teacher = relationship('User', back_populates='created_courses')
    materials = relationship('Material', back_populates='course', cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_course_title', 'title'), # Добавим индекс на title для поиска
        Index('idx_course_category_level', 'category', 'level'),
    )

    def __repr__(self):
        return f"<Course(id={self.id}, title='{self.title}')>"

class Material(Base):
    __tablename__ = 'materials'
    
    id = Column(Integer, primary_key=True, index=True)
    course_id = Column(Integer, ForeignKey('courses.id', ondelete="CASCADE"), nullable=False) # ondelete добавлен
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=True) # nullable=True если контент не обязателен
    type = Column(String(20), index=True, nullable=False) # 'video', 'text', 'quiz', 'assignment'
    order_index = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    course = relationship('Course', back_populates='materials')
    activities = relationship('Activity', back_populates='material', cascade="all, delete-orphan") # cascade добавлен

    def __repr__(self):
        return f"<Material(id={self.id}, title='{self.title}', type='{self.type}')>"

class Activity(Base):
    __tablename__ = 'activities'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete="CASCADE"), nullable=False) # ondelete добавлен
    material_id = Column(Integer, ForeignKey('materials.id', ondelete="CASCADE"), nullable=False) # ondelete добавлен
    action = Column(String(50), index=True, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)
    duration = Column(Float, nullable=True) # в секундах
    score = Column(Float, nullable=True) # 0.0 to 1.0 or 0 to 100, etc.
    meta = Column(JSON, nullable=True)
    
    user = relationship('User', back_populates='activities')
    material = relationship('Material', back_populates='activities')
    
    __table_args__ = (
        Index('idx_activity_user_material', 'user_id', 'material_id'), # Более полезный индекс
        Index('idx_activity_user_action', 'user_id', 'action'),
        # Index('idx_activity_material_timestamp', 'material_id', 'timestamp'), # Этот был, но user_material может быть полезнее
    )

    def __repr__(self):
        return f"<Activity(id={self.id}, user_id={self.user_id}, material_id={self.material_id}, action='{self.action}')>"