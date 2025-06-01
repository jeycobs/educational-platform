# main.py
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from io import StringIO
import csv
import json # Для ETL экспорта meta поля

from fastapi import FastAPI, Query, HTTPException, Depends, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse

from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, Field # Field используется в новом main.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import or_, and_, func, desc # desc может понадобиться для сортировки
from collections import Counter

# --- Локальные импорты ---
# Убедитесь, что db.py соответствует моделям, которые ожидает новый интерфейс
# (например, User.email, Course.description, Material.content, Material.order_index)
from db import SessionLocal, User as DBUser, Course as DBCourse, Material as DBMaterial, Activity as DBActivity

# Whoosh Search Service
from search_service import (
    init_whoosh_indexes,
    index_course_item,
    index_material_item,
    index_teacher_item,
    delete_item_from_index,
    search_whoosh,
    ix_courses,
    ix_materials,
    ix_teachers,
    INDEX_DIR
)
from whoosh import index as whoosh_index # Для очистки индекса, если потребуется

# --- Настройки приложения ---
SECRET_KEY = "your-secret-key-here"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token") # Эндпоинт /token ниже

app = FastAPI(
    title="Online Courses Platform API",
    description="Educational platform with course management and analytics",
    version="1.0.0"
)

# --- MIDDLEWARE ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- СТАТИЧЕСКИЕ ФАЙЛЫ И ШАБЛОНЫ ---
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --- СОБЫТИЯ FastAPI ---
@app.on_event("startup")
async def startup_event():
    print("FastAPI application startup...")
    try:
        init_whoosh_indexes() # Инициализация Whoosh
        print("Whoosh indexes are ready.")
    except Exception as e:
        print(f"CRITICAL ERROR during Whoosh initialization on startup: {e}")
        import traceback
        traceback.print_exc()


# --- PYDANTIC МОДЕЛИ (Схемы) ---
# Используем модели из "нового" main.py, так как они более полные
# и соответствуют ожидаемой структуре в app.js

class Token(BaseModel):
    access_token: str
    token_type: str

class UserBase(BaseModel): # Добавил UserBase для консистентности
    name: str
    email: str # В "новом" main.py email есть
    role: str

class UserCreate(UserBase):
    password: str = Field(min_length=6)

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = Field(None, pattern="^(student|teacher|admin)$")
    password: Optional[str] = Field(None, min_length=6)

class UserInDB(UserBase): # Переименовал User в UserInDB для ясности, что это модель ответа
    id: int
    is_active: bool = True # Добавлено из нового main.py
    created_at: datetime
    class Config:
        from_attributes = True

class CourseBase(BaseModel): # Добавил CourseBase
    title: str
    description: Optional[str] = None # Из нового main.py
    category: str
    level: str
    teacher_id: int

class CourseCreate(CourseBase):
    level: str = Field(pattern="^(beginner|intermediate|advanced)$")

class CourseUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    level: Optional[str] = Field(None, pattern="^(beginner|intermediate|advanced)$")
    # teacher_id можно не включать, если он не меняется через этот эндпоинт,
    # или добавить, если новый интерфейс позволяет менять преподавателя курса

class CourseInDB(CourseBase): # Переименовал Course
    id: int
    created_at: datetime
    class Config:
        from_attributes = True

class MaterialBase(BaseModel): # Добавил MaterialBase
    title: str
    content: Optional[str] = None # Из нового main.py
    type: str
    course_id: int
    order_index: int = 0 # Из нового main.py

class MaterialCreate(MaterialBase):
    type: str = Field(pattern="^(video|text|quiz|assignment)$")

class MaterialUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    type: Optional[str] = Field(None, pattern="^(video|text|quiz|assignment)$")
    order_index: Optional[int] = None
    # course_id можно не включать, если не планируется перемещение материалов между курсами

class MaterialInDB(MaterialBase): # Переименовал Material
    id: int
    class Config:
        from_attributes = True

class ActivityBase(BaseModel):
    user_id: int # Из старого main.py
    material_id: int # Из старого main.py
    action: str
    timestamp: datetime # Из старого main.py
    duration: Optional[float] = None
    score: Optional[float] = None
    meta: Optional[Dict[str, Any]] = None # Было dict, стало Dict

class ActivityCreate(BaseModel): # Из нового main.py, но адаптируем
    user_id: int # Для явного указания кем логируется
    material_id: int
    action: str
    duration: Optional[float] = None
    score: Optional[float] = None
    meta: Optional[Dict[str, Any]] = None


class ActivityInDB(ActivityBase): # Переименовал Activity
    id: int
    class Config:
        from_attributes = True

# Модели для поиска (из старого Whoosh-совместимого main.py)
class SearchResultItem(BaseModel):
    id: int
    title: str
    type: str
    category: Optional[str] = None
    level: Optional[str] = None
    teacher_name: Optional[str] = None
    material_type_field: Optional[str] = None # Было material_type, но это конфликтовало бы с полем type
    relevance_score: Optional[float] = None
    # Дополнительные поля из нового интерфейса, если они возвращаются поиском
    description: Optional[str] = None 

class SearchResponse(BaseModel):
    query: Optional[str]
    filters: Dict[str, Optional[str]] # Было в Whoosh-версии
    results: List[SearchResultItem]
    # Новый интерфейс ожидает total_courses, total_materials. Добавим их, если будем возвращать так из поиска.
    # Либо сам поиск Whoosh будет возвращать только один список results, и фронтенд должен будет его обработать.
    # Пока оставим как было в Whoosh-версии, фронтенд нужно будет адаптировать к этому.


# --- ЗАВИСИМОСТИ ---
async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback() # Важно для отката транзакций при ошибках
            raise
        finally:
            await session.close()

# --- АУТЕНТИФИКАЦИЯ И АВТОРИЗАЦИЯ (хелперы) ---
# (Код verify_password, get_password_hash, create_access_token, get_current_user_dependency, require_role 
#  остается таким же, как в вашем "старом" main.py или "новом", они идентичны по функционалу)
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire_time = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire_time, "sub": str(data.get("sub"))}) # Убедимся, что sub - строка
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)) -> DBUser: # Переименовал в get_current_user
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str: Optional[str] = payload.get("sub")
        if user_id_str is None:
            raise credentials_exception
        user_id = int(user_id_str) # Преобразуем в int
    except (JWTError, ValueError):
        raise credentials_exception
    
    result = await db.execute(select(DBUser).where(DBUser.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        raise credentials_exception
    return user

def require_role(*roles: str):
    async def role_checker(current_user: DBUser = Depends(get_current_user)): # Используем get_current_user
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"User with role '{current_user.role}' does not have permission for roles: {roles}",
            )
        return current_user
    return role_checker

# --- Frontend routes (из "нового" main.py) ---
@app.get("/", response_class=HTMLResponse, tags=["Frontend"])
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse, tags=["Frontend"])
async def dashboard(request: Request, current_user: DBUser = Depends(get_current_user)): # Дашборд требует авторизации
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": current_user})


# --- ЭНДПОИНТЫ API ---

# Auth (из "нового" main.py, но логика идентична)
@app.post("/token", response_model=Token, tags=["Auth"])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    # В новом app.js используется email как username
    result = await db.execute(select(DBUser).where(DBUser.email == form_data.username)) # Изменено name на email
    user = result.scalar_one_or_none()
    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password")
    access_token_data = {"sub": user.id, "role": user.role, "name": user.name, "email": user.email} # Добавим больше инфо в токен, если нужно
    access_token = create_access_token(data=access_token_data)
    return {"access_token": access_token, "token_type": "bearer"}

# Users
@app.post("/register", response_model=UserInDB, status_code=status.HTTP_201_CREATED, tags=["Users"]) # Эндпоинт из "нового" main.py
async def register_new_user(user_in: UserCreate, db: AsyncSession = Depends(get_db)): # user -> user_in
    result = await db.execute(select(DBUser).where(DBUser.email == user_in.email)) # Используем email
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already registered")
    
    hashed_password = get_password_hash(user_in.password)
    db_user = DBUser(
        name=user_in.name,
        email=user_in.email, # Добавлено
        role=user_in.role,
        password_hash=hashed_password,
        created_at = datetime.utcnow() # Убедимся что created_at для User тоже есть в модели DBUser
    )
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    if db_user.role == "teacher":
        index_teacher_item(db_id=db_user.id, name=db_user.name) # Индексация Whoosh
    return db_user

@app.get("/users/me", response_model=UserInDB, tags=["Users"]) # Эндпоинт из "нового" main.py
async def read_current_user(current_user: DBUser = Depends(get_current_user)):
    return current_user

# Get all users (для админа/преподавателя)
@app.get("/users", response_model=List[UserInDB], tags=["Users"])
async def get_all_users( # Переименовал get_users -> get_all_users
    db: AsyncSession = Depends(get_db),
    current_user: DBUser = Depends(require_role("admin", "teacher")),
    skip: int = 0,
    limit: int = 100
):
    result = await db.execute(select(DBUser).offset(skip).limit(limit))
    return result.scalars().all()


# Course management (интегрируем Whoosh индексацию)
@app.post("/courses", response_model=CourseInDB, status_code=status.HTTP_201_CREATED, tags=["Courses"])
async def create_course_endpoint( # Переименовал
    course_in: CourseCreate, # course -> course_in
    db: AsyncSession = Depends(get_db),
    current_user: DBUser = Depends(require_role("admin", "teacher"))
):
    teacher_res = await db.execute(select(DBUser).where(DBUser.id == course_in.teacher_id, DBUser.role == "teacher"))
    teacher = teacher_res.scalar_one_or_none()
    if not teacher:
        raise HTTPException(status_code=400, detail=f"Teacher with id {course_in.teacher_id} not found or is not a teacher.")
    
    # Добавляем created_at, если его нет в CourseCreate, но есть в модели DBCourse
    db_course = DBCourse(**course_in.dict(), created_at=datetime.utcnow()) 
    db.add(db_course)
    await db.commit()
    await db.refresh(db_course)
    # Whoosh indexing
    index_course_item(db_id=db_course.id, title=db_course.title, category=db_course.category, level=db_course.level, teacher_name=teacher.name)
    return db_course

@app.get("/courses", response_model=List[CourseInDB], tags=["Courses"])
async def get_courses_endpoint( # Переименовал
    db: AsyncSession = Depends(get_db),
    # current_user: DBUser = Depends(get_current_user), # Сделаем публичным для нового интерфейса
    skip: int = 0,
    limit: int = 100,
    category: Optional[str] = None,
    level: Optional[str] = None
):
    stmt = select(DBCourse).order_by(desc(DBCourse.created_at)).offset(skip).limit(limit) # Сортировка по дате
    if category:
        stmt = stmt.where(DBCourse.category.ilike(f"%{category}%")) # ilike для частичного совпадения
    if level:
        stmt = stmt.where(DBCourse.level == level)
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/courses/{course_id}", response_model=CourseInDB, tags=["Courses"])
async def get_course_endpoint( # Переименовал
    course_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: DBUser = Depends(get_current_user) # Сделаем публичным
):
    result = await db.execute(select(DBCourse).where(DBCourse.id == course_id))
    course = result.scalar_one_or_none()
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    return course

@app.patch("/courses/{course_id}", response_model=CourseInDB, tags=["Courses"])
async def update_course_endpoint( # Переименовал
    course_id: int, 
    course_update: CourseUpdate, # course -> course_update
    db: AsyncSession = Depends(get_db), 
    current_user: DBUser = Depends(require_role("admin", "teacher"))
):
    result = await db.execute(select(DBCourse).options(selectinload(DBCourse.teacher)).where(DBCourse.id == course_id))
    db_course = result.scalar_one_or_none()
    if not db_course:
        raise HTTPException(status_code=404, detail="Course not found")
    if current_user.role == "teacher" and db_course.teacher_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to update this course")
    
    update_data = course_update.dict(exclude_unset=True)
    teacher_name_for_index = db_course.teacher.name if db_course.teacher else "" # Инициализация

    for key, value in update_data.items():
        setattr(db_course, key, value)
        # teacher_id не меняется через этот эндпоинт в "новом" main.py,
        # если нужно, добавьте логику как в "старом" для обновления teacher_name_for_index
        # В CourseUpdate из нового main.py нет teacher_id.

    await db.commit()
    await db.refresh(db_course)
    
    # Получаем актуальное имя преподавателя, если оно могло измениться (хотя в этой версии teacher_id не меняется)
    if db_course.teacher_id:
        current_teacher_res = await db.execute(select(DBUser.name).where(DBUser.id == db_course.teacher_id))
        teacher_name_for_index = current_teacher_res.scalar_one_or_none() or ""
    
    index_course_item(db_id=db_course.id, title=db_course.title, category=db_course.category, level=db_course.level, teacher_name=teacher_name_for_index)
    return db_course

@app.delete("/courses/{course_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Courses"])
async def delete_course_endpoint( # Переименовал
    course_id: int, 
    db: AsyncSession = Depends(get_db), 
    current_user: DBUser = Depends(require_role("admin", "teacher"))
):
    result = await db.execute(select(DBCourse).where(DBCourse.id == course_id))
    db_course = result.scalar_one_or_none()
    if not db_course:
        raise HTTPException(status_code=404, detail="Course not found")
    if current_user.role == "teacher" and db_course.teacher_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to delete this course")
    
    delete_item_from_index(ix_courses, db_id=course_id)
    materials_of_course_res = await db.execute(select(DBMaterial.id).where(DBMaterial.course_id == course_id))
    for mat_id_tuple in materials_of_course_res.all():
        delete_item_from_index(ix_materials, db_id=mat_id_tuple[0])
    
    await db.delete(db_course)
    await db.commit()


# Material management (интегрируем Whoosh индексацию)
@app.post("/materials", response_model=MaterialInDB, status_code=status.HTTP_201_CREATED, tags=["Materials"])
async def create_material_endpoint( # Переименовал
    material_in: MaterialCreate, # material -> material_in
    db: AsyncSession = Depends(get_db),
    current_user: DBUser = Depends(require_role("admin", "teacher"))
):
    course_res = await db.execute(select(DBCourse).where(DBCourse.id == material_in.course_id))
    course = course_res.scalar_one_or_none()
    if not course:
        raise HTTPException(status_code=400, detail=f"Course with id {material_in.course_id} not found.")
    if current_user.role == "teacher" and course.teacher_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized to add material to this course")
    
    db_material = DBMaterial(**material_in.dict())
    db.add(db_material)
    await db.commit()
    await db.refresh(db_material)
    index_material_item(db_id=db_material.id, title=db_material.title, material_type=db_material.type, course_id_ref=db_material.course_id, course_title_ref=course.title)
    return db_material

@app.get("/materials", response_model=List[MaterialInDB], tags=["Materials"])
async def get_materials_endpoint( # Переименовал
    db: AsyncSession = Depends(get_db),
    # current_user: DBUser = Depends(get_current_user), # Сделаем публичным для нового интерфейса
    course_id: Optional[int] = None, # Фильтр из нового main.py
    skip: int = 0, # Добавил пагинацию
    limit: int = 100
):
    stmt = select(DBMaterial).order_by(DBMaterial.order_index).offset(skip).limit(limit)
    if course_id:
        stmt = stmt.where(DBMaterial.course_id == course_id)
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/courses/{course_id}/materials", response_model=List[MaterialInDB], tags=["Materials"]) # Этот эндпоинт дублирует функционал GET /materials?course_id=...
async def get_course_materials_endpoint( # Переименовал
    course_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: DBUser = Depends(get_current_user) # Публичный доступ
):
    result = await db.execute(select(DBMaterial).where(DBMaterial.course_id == course_id).order_by(DBMaterial.order_index))
    materials = result.scalars().all()
    if not materials and not await db.scalar(select(DBCourse.id).where(DBCourse.id == course_id)):
        raise HTTPException(status_code=404, detail="Course not found")
    return materials


# Search functionality (используем Whoosh из старого main.py)
@app.get("/search", response_model=SearchResponse, tags=["Search"])
async def search_endpoint( # Переименовал
    q: Optional[str] = Query(None, alias="query", description="Search query"), # alias="query" для совместимости с js
    category: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    material_type: Optional[str] = Query(None), # Добавлено
    teacher_name: Optional[str] = Query(None), # Добавлено
    # Параметры из старого frontend.html для Whoosh
    search_in_courses: bool = Query(True, description="Search in courses"),
    search_in_materials: bool = Query(True, description="Search in materials"),
    search_in_teachers: bool = Query(True, description="Search in teachers"),
    limit: int = Query(20, ge=1, le=100) # Увеличил лимит
):
    # В "новом" app.js поиск отправляет `q` и фильтры. `search_in_*` нет.
    # Адаптируем вызов search_whoosh, предполагая, что если `q` есть, ищем везде,
    # а фильтры применяются к соответствующим типам.
    # Либо нужно будет менять app.js, чтобы он отправлял `search_in_*`.
    # Пока сделаем так, что если `q` есть, ищем везде по умолчанию, если не указано иное.
    
    # Для совместимости с новым интерфейсом, где нет чекбоксов search_in_*:
    # Если q есть, ищем везде. Если q нет, а есть фильтры, то ищем только там, где фильтры применимы.
    effective_search_in_courses = search_in_courses
    effective_search_in_materials = search_in_materials
    effective_search_in_teachers = search_in_teachers

    if not q: # Если нет основного запроса, уточняем где искать на основе фильтров
        if category or level: # Фильтры для курсов
            effective_search_in_materials = False
            effective_search_in_teachers = False
        elif material_type: # Фильтр для материалов
            effective_search_in_courses = False
            effective_search_in_teachers = False
        elif teacher_name: # Фильтр для преподавателей
            effective_search_in_courses = False
            effective_search_in_materials = False
        elif not any([category,level,material_type,teacher_name]): # Нет ни q, ни фильтров
             raise HTTPException(status_code=400, detail="Please provide a search query or at least one filter.")


    results_from_whoosh = search_whoosh(
        query_str=q,
        search_in_courses=effective_search_in_courses,
        search_in_materials=effective_search_in_materials,
        search_in_teachers=effective_search_in_teachers,
        filter_category=category,
        filter_level=level,
        filter_material_type=material_type,
        filter_teacher_name=teacher_name, # Передаем teacher_name в Whoosh
        limit=limit
    )
    applied_filters = {"category": category, "level": level, "material_type": material_type, "teacher_name": teacher_name}
    pydantic_results = [SearchResultItem(**item) for item in results_from_whoosh]
    
    # Новый интерфейс может ожидать раздельные списки. Адаптируем или меняем интерфейс.
    # Пока возвращаем как SearchResponse из Whoosh версии.
    return SearchResponse(query=q, filters=applied_filters, results=pydantic_results)


# Admin Search Utils (из старого Whoosh-совместимого main.py)
async def _perform_full_reindex(db: AsyncSession):
    print("Starting full reindex of all data for Whoosh...")
    print(f"Attempting to clear Whoosh index at {INDEX_DIR.resolve()}...")
    if INDEX_DIR.exists():
        import shutil
        for item in INDEX_DIR.iterdir():
            if item.is_dir():
                print(f"Removing directory: {item}")
                shutil.rmtree(item)
            else:
                print(f"Removing file: {item}")
                item.unlink()
        print("Whoosh index directory content cleared (if any).")
    init_whoosh_indexes()

    courses_res = await db.execute(select(DBCourse).options(selectinload(DBCourse.teacher)))
    course_count = 0
    for course in courses_res.scalars().all():
        teacher_name_val = course.teacher.name if course.teacher else None
        # Убедимся, что передаем все необходимые поля для индексации курса
        # В search_service.py для index_course_item ожидается db_id, title, category, level, teacher_name
        index_course_item(course.id, course.title, course.category, course.level, teacher_name_val)
        course_count+=1
    print(f"Reindexed {course_count} courses.")

    materials_res = await db.execute(select(DBMaterial).options(selectinload(DBMaterial.course)))
    material_count = 0
    for material in materials_res.scalars().all():
        course_title_val = material.course.title if material.course else "N/A"
        # Для index_material_item: db_id, title, material_type, course_id_ref, course_title_ref
        index_material_item(material.id, material.title, material.type, material.course_id, course_title_val)
        material_count +=1
    print(f"Reindexed {material_count} materials.")

    teachers_res = await db.execute(select(DBUser).where(DBUser.role == 'teacher'))
    teacher_count = 0
    for teacher in teachers_res.scalars().all():
        # Для index_teacher_item: db_id, name
        index_teacher_item(teacher.id, teacher.name)
        teacher_count+=1
    print(f"Reindexed {teacher_count} teachers.")
    print("Full reindex completed successfully.")

@app.post("/admin/search/reindex-all", status_code=status.HTTP_202_ACCEPTED, tags=["Admin Search Utils"])
async def trigger_full_reindex_endpoint(
    db: AsyncSession = Depends(get_db),
    current_user: DBUser = Depends(require_role("admin"))
):
    try:
        await _perform_full_reindex(db)
        return {"message": "Full reindexing process completed (synchronously)."}
    except Exception as e:
        print(f"Error during reindexing: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Reindexing failed: {str(e)}")


# Activity logging (из "нового" main.py, но адаптируем user_id)
@app.post("/activities", response_model=ActivityInDB, tags=["Activity"]) # /activities как в app.js
async def create_activity_endpoint( # Переименовал
    activity_in: ActivityCreate, # activity -> activity_in
    db: AsyncSession = Depends(get_db),
    current_user: DBUser = Depends(get_current_user) # get_current_user вместо get_current_user_dependency
):
    # Проверка, что пользователь логирует свою активность или это админ/преподаватель
    # В ActivityCreate user_id передается явно, что хорошо для логирования от имени другого (админом)
    if activity_in.user_id != current_user.id and current_user.role not in ["admin", "teacher"]:
         raise HTTPException(status_code=403, detail="Not authorized to log activity for another user unless admin/teacher")
    
    material_exists = await db.execute(select(DBMaterial.id).where(DBMaterial.id == activity_in.material_id))
    if not material_exists.scalar_one_or_none():
        raise HTTPException(status_code=404, detail=f"Material with id {activity_in.material_id} not found.")

    db_activity = DBActivity(
        **activity_in.dict(), # Сохраняем все поля из ActivityCreate
        timestamp=datetime.utcnow() # Устанавливаем timestamp на сервере
    )
    db.add(db_activity)
    await db.commit()
    await db.refresh(db_activity)
    return db_activity


# Analytics endpoints (из "нового" main.py)
@app.get("/analytics/user/{user_id}/progress", tags=["Analytics"]) # URL совпадает с app.js
async def get_user_progress_endpoint( # Переименовал
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: DBUser = Depends(get_current_user) # Проверка прав
):
    if user_id != current_user.id and current_user.role not in ["admin", "teacher"]:
        raise HTTPException(status_code=403, detail="Not authorized to view progress for another user")

    stmt = select(DBActivity, DBMaterial, DBCourse).join(
        DBMaterial, DBActivity.material_id == DBMaterial.id
    ).join(
        DBCourse, DBMaterial.course_id == DBCourse.id
    ).where(DBActivity.user_id == user_id)
    
    result = await db.execute(stmt)
    activities_data = result.all()
    
    course_progress_map = {} # Используем map для избежания конфликта с Pydantic Course
    for activity, material, course_obj in activities_data:
        if course_obj.id not in course_progress_map:
            # Получаем общее количество материалов в курсе один раз
            materials_count_stmt = select(func.count(DBMaterial.id)).where(DBMaterial.course_id == course_obj.id)
            total_materials_in_course = (await db.execute(materials_count_stmt)).scalar_one_or_none() or 0
            
            course_progress_map[course_obj.id] = {
                "course_id": course_obj.id, # Добавим ID для ключа, если понадобится
                "course_title": course_obj.title,
                "total_materials": total_materials_in_course,
                "completed_materials": 0,
                "total_time": 0.0,
                "scores": []
            }
        
        progress_item = course_progress_map[course_obj.id]
        progress_item["total_time"] += activity.duration or 0
        if activity.action == "complete": # Считаем завершенным, если есть такая активность
            progress_item["completed_materials"] += 1
        if activity.score is not None:
            progress_item["scores"].append(activity.score)
    
    final_progress_list = []
    for course_id_key, progress_data in course_progress_map.items():
        progress_data["completion_percentage"] = (
            (progress_data["completed_materials"] / progress_data["total_materials"]) * 100
            if progress_data["total_materials"] > 0 else 0
        )
        progress_data["avg_score"] = (
            sum(progress_data["scores"]) / len(progress_data["scores"])
            if progress_data["scores"] else None # None если нет оценок
        )
        del progress_data["scores"]
        final_progress_list.append(progress_data)
        
    return final_progress_list # app.js ожидает объект, не список. Адаптируем или app.js
                               # Пока возвращаем список, app.js может перебрать его


# ETL endpoints (из "нового" main.py)
@app.get("/etl/activities/export", response_class=StreamingResponse, tags=["ETL & Data Preparation"]) # URL совпадает
async def export_activities_csv_endpoint( # Переименовал
    db: AsyncSession = Depends(get_db),
    current_user: DBUser = Depends(require_role("admin"))
):
    stmt = select(DBActivity, DBUser, DBMaterial, DBCourse).join(
        DBUser, DBActivity.user_id == DBUser.id
    ).join(
        DBMaterial, DBActivity.material_id == DBMaterial.id
    ).join(
        DBCourse, DBMaterial.course_id == DBCourse.id
    )
    result = await db.execute(stmt)
    
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "user_id", "user_name", "user_email", "course_id", "course_title",
        "material_id", "material_title", "material_type", "action",
        "timestamp", "duration", "score", "meta"
    ])
    for activity, user, material, course_obj in result.all():
        writer.writerow([
            user.id, user.name, user.email, course_obj.id, course_obj.title, # user.email из нового User
            material.id, material.title, material.type, activity.action,
            activity.timestamp.isoformat(), activity.duration, activity.score, # .isoformat() для datetime
            json.dumps(activity.meta) if activity.meta else "" # json.dumps для meta
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=activities_export.csv"})

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000) # Обычно uvicorn запускается из командной строки